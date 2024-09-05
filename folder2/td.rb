#!/usr/local/rbenv/shims/ruby
require 'logger'
require 'csv'
require 'json'
require 'nokogiri'
require 'tiny_tds'
require './config'
require './Common'

DEBUGGING=false

class TDRUBY < TWU
	def initialize
		@scriptpath = "/home/jus/bin"
		@logPath = "#{@scriptpath}/log.txt"
		@stdlog = Logger.new(STDOUT)
		@filelog = Logger.new(@logPath)
		@dblog = File.new("#{@scriptpath}/dblog.txt", 'a')
		@targetdb = { :username => DBUSER, :password => DBPWD, :host => DBHOST, :database => DATABASE, :timeout => DBTIMEOUT, :message_handler => Proc.new{|m| log(:info, m.message)} }
		@rootpath =  "/home/share/#{PROJECT}/#{ENVIRONMENT}"
		@sandboxroot = "/home/tomcat/CloverDX/sandboxes/IT_Prod"
		@db = nil

		@fields = {}
		@fields['Ticket'] = {
			'AppID' => '',
			'ID' => '',
			'Title' => '',
			'Description' => '',
			'TypeName' => '',
			'TypeCategoryName' => '',
			'ClassificationName' => '',
			'FormName' => '',
			'AccountName' => '',
			'SourceName' => '',
			'StatusName' => '',
			'SlaName' => '',
			'ResponsibleGroupName' => '',
			'ServiceName' => '',
			'ServiceCategoryName' => '',
			'CreatedDate' => '',
			'CompletedDate' => ''
		}
		@fields['Feed'] = {
			'ItemID' => '',
			'ID' => '',
			'Body' => ''
		}
		super()
	end
	def pull_td(entity, id=nil, parameters={}) # Run TeamDynamix Pull.
		profilepath = "#{@sandboxroot}/profile"
		intermediate_file = "#{@rootpath}/Import/Working/#{entity}.json"
		copyFile("#{profilepath}/TeamDynamix.Pull.#{entity}.prm", "#{profilepath}/TeamDynamix.Pull.prm")
		parameters["ENTITY_ID"] = id if id != nil
		parameters["OUTPUT_FILE"] = intermediate_file
		runId, runStatus = runGraph("TeamDynamix.Pull.grf", parameters)
		return intermediate_file
	end
	def pull_td2(entity, id=nil, parameters={}) # Run TeamDynamix Pull2.
		profilepath = "#{@sandboxroot}/profile"
		intermediate_file = "#{@rootpath}/Import/Working/#{entity}.json"
		copyFile("#{profilepath}/TeamDynamix.Pull.#{entity}.prm", "#{profilepath}/TeamDynamix.Pull2.prm")
		parameters["ENTITY_ID"] = id if id != nil
		parameters["OUTPUT_FILE"] = intermediate_file
		runId, runStatus = runGraph("TeamDynamix.Pull2.grf", parameters)
		return intermediate_file
	end
	def refresh_tickets # Refresh td tickets in db.
		entity = 'Ticket'
		accounts_to_pull = [ 39036 ] # 39036 - Student tickets.
		maxresults = 1999
		totaldownloaded = 0

		sql = "TRUNCATE TABLE Staging.TD#{entity}"
		runsql(sql)

		# Download tickets by month.
		11.downto(1) do |m|
			from_date = DateTime.new(2021, m, 1)
			to_date = DateTime.new(2021, m+1, 1)

			# Pull tickets for the whole month for each App Id.
			[366, 1220, 3049, 478, 2637, 2391].each do |appId|
				#parameters = { "REQUEST_CONTENT" => "{MaxResults:#{maxresults},AccountIDs:#{accounts_to_pull.to_s},CreatedDateFrom:'#{from_date.iso8601}',CreatedDateTo:'#{to_date.iso8601}'}" }
				parameters = { "MODEL_ID" => "#{appId}", "REQUEST_CONTENT" => "{MaxResults:#{maxresults},CreatedDateFrom:'#{from_date.iso8601}',CreatedDateTo:'#{to_date.iso8601}'}" }
				tickets_file = pull_td('Tickets', nil, parameters)

				# Save ID and AppID.
				j = JSON.parse(File.new(tickets_file).read)
				tickets = j.map{|x| "('#{x['ID']}', '#{x['AppID']}')"}
				puts "\n\n\nSize: #{tickets.size}\n\n\n"
				totaldownloaded += tickets.size
				tickets.each_slice(100) do |t|
					sql = "INSERT INTO Staging.TD#{entity} (ID,AppID) VALUES #{t.join(',')}"
					runsql(sql)
				end

				sleep 2
			end
		end
		puts "\n\n\nTotal downloaded: #{totaldownloaded}"
	end
	def refresh_feeds # Refresh td feeds in db.
		sql = "TRUNCATE TABLE Staging.TDFeed"
		runsql(sql)
	end
	def pull_tickets # Pull td tickets.
		entity = 'Ticket'
		@db = TinyTds::Client.new(@targetdb)
		sql = "SELECT AppID, ID FROM Staging.TD#{entity} WHERE Title IS NULL AND AppID != '3049' ORDER BY AppID, ID DESC"
		rows = @db.execute(sql)
		tuples = rows.map{|x| [ x['AppID'], x['ID'] ]}
		rows.cancel

		tuples.each do |tuple|
			appId, id = tuple

			# Pull ticket.
			parameters = { "MODEL_ID" => "#{appId}" }
			ticket_file = pull_td(entity, id, parameters)

			# Prepare field values.
			ticketstr = File.new(ticket_file).read
			if ticketstr && !ticketstr.to_s.strip.empty?
				ticket = JSON.parse(ticketstr)
				@fields[entity].keys.each{ |x|
					@fields[entity][x] = ticket[x].to_s
						.gsub(/'/, "''")
						.gsub(/\s+/, ' ')
				}

				# Update db.
				sql = "UPDATE Staging.TD#{entity} SET #{@fields[entity].to_a.map{|x| "#{x[0]} = '#{x[1]}'"}.join(', ')} WHERE AppID = '#{appId}' AND ID = '#{id}'"
				@db.execute(sql).do
			end

			sleep 1
		end
		@db.close
	end
	def cleanup(feedbody) # Clean up feed body.
		result = feedbody
			.gsub(/<.*?>/, '')
			.gsub(/Changed [^\.]*from [^\.]*to .*?\./, '')
			.gsub(/Added .*?\./, '')
			.gsub(/Assigned [^\.]*to .*?\./, '')
			.gsub(/Reassigned [^\.]*from [^\.]*to .*?\./, '')
			.gsub(/Merged .*?\)[:\.]/, '')
			.gsub(/\[Merged from .*?\]/, '')
			.gsub(/Took primary responsibility .*?\./, '')
			.gsub(/Posted By: .*?>/, '')
			.gsub(/This message was proxied .*>\./, '')
			.gsub(/\[Created from .*?\]/, '')
			.gsub(/(\\r\\n|\\r|\\n)+/, ' ')
			.gsub(/(\s|\t)+/, ' ')
			.gsub(/&#39;/, "'")
			.gsub(/&lt;/, '<')
			.gsub(/&gt;/, '>')
			.gsub(/&quot;/, '"')
			.gsub(/&amp;/, '&')
		return result
	end
	def pull_feeds # Pull td feeds.
		@db = TinyTds::Client.new(@targetdb)
		sql = "SELECT t.AppID, t.ID FROM Staging.TDTicket t LEFT JOIN Staging.TDFeed f ON t.ID = f.ItemID WHERE COALESCE(t.ID, '') != '' AND f.ID IS NULL ORDER BY t.AppID, t.ID DESC"
		rows = @db.execute(sql)
		tuples = rows.map{|x| [ x['AppID'], x['ID'] ]}
		rows.cancel

		tuples.each do |tuple|
			appId, id = tuple

			# Pull feeds.
			parameters = { "MODEL_ID" => "#{appId}" }
			ticketfeeds_file = pull_td2('TicketFeeds', id, parameters)
			allfeedstr = File.new(ticketfeeds_file).read
			if allfeedstr && !allfeedstr.to_s.strip.empty? && JSON.parse(allfeedstr)
				ticketfeeds = JSON.parse(allfeedstr)
				ticketfeeds = [] if ticketfeeds == nil
				ticketfeeds.each do |f|
					entity = 'Feed'
					feedbody = []
					feed_file = pull_td2(entity, f['ID'])

					feedstr = File.new(feed_file).read
					if feedstr && !feedstr.to_s.strip.empty? && feedstr.length
						# Concat feed body and replies together.
						feed = JSON.parse(feedstr)
						#feedbody.push(cleanup(feed['Body']))
						feedbody.push(feed['Body'])
						replies = feed['Replies']
						#replies.each{ |r| feedbody.push(cleanup(r['Body'])) }
						replies.each{ |r| feedbody.push(r['Body']) }

						if feedbody.join("\n").size > 0 #&& !(feedbody =~ /^(Resolved|Problem solved.)$/i)
							# Prepare field values.
							@fields[entity]['ItemID'] = id
							@fields[entity]['ID'] = f['ID']
							@fields[entity]['Body'] = "'#{feedbody.join("\n").gsub(/'/, "''")}'"

							# Update db.
							sql = "DELETE Staging.TD#{entity} WHERE ID = '#{id}';INSERT INTO Staging.TD#{entity} (#{@fields[entity].keys.join(',')}) VALUES (#{@fields[entity].values.join(',')})"
							@db.execute(sql).do
						end
					end

					sleep 1
				end
			end
		end
		@db.close
	end

	def push_td(pushToTarget=nil) # Push items in the working folder to TD.
		require 'json'

		# Local init.
		@topic = "Perform Ops Proposal"
		@robotname = "Simple Ops Robot"
		@signature = "<br /><p>Yours truly,<br/><br/><span style=\"font-size:1.4em;font-family:'Brush Script MT',Phyllis,'Lucida Handwriting',cursive;\">#{@robotname}</span></p>"

		prepareFolders("Import")

		@emailTo.push("juswaldy.jusman@twu.ca")

		# Go through each Push file, amend it, and push to TeamDynamix.
		profilepath = "#{@sandboxroot}/profile"
		intermediateFile = "/tmp/Telus2TeamDynamix_Op.json"
		errors = []

		# Retired.
		copyFile("#{profilepath}/TeamDynamix.Update.Asset.prm", "#{profilepath}/TeamDynamix.Push.prm")
		retiredFiles = Dir.glob("#{@rootpath}/Import/Working/Retired_*.json")
		retiredFiles.each do |f|
			j = JSON.parse(File.new(f).read)
			entityId = j['ID']
			# Push to TeamDynamix
			parameters = {
				"ENTITY_ID" => entityId,
				"APICALL_PATH" => "${MODEL_ID}/assets/${ENTITY_ID}",
				"INPUT_FILE" => f
			}
			runId, runStatus = runGraph("TeamDynamix.Push.grf", parameters) if pushToTarget
			errors.push(runStatus) if runStatus != "FINISHED_OK"
		end

		# New.
		copyFile("#{profilepath}/TeamDynamix.Insert.Asset.prm", "#{profilepath}/TeamDynamix.Push.prm")
		newFiles = Dir.glob("#{@rootpath}/Import/Working/New_*.json")
		newFiles.each do |f|
			# Push to TeamDynamix.
			parameters = {
				"ENTITY_ID" => "",
				"APICALL_PATH" => "${MODEL_ID}/assets",
				"INPUT_FILE" => f
			}
			runId, runStatus = runGraph("TeamDynamix.Push.grf", parameters) if pushToTarget
			errors.push(runStatus) if runStatus != "FINISHED_OK"
		end

		# Clean up intermediate files.
#jjj				removeFiles([retiredFiles, newFiles, updatedFiles, intermediateFile].flatten)

#		# If FINISHED_OK then wrap up.
#		if runStatus == "FINISHED_OK"
#			# Archive the files.
#			archiveFiles(filelist)
#
#			# Prepare status message.
#			if errors.size == 0
#				statusMessage = "The Staging DB and TeamDynamix app has been updated accordingly. Please verify the results in the TeamDynamix app, and make sure everything looks good ok? :)"
#			else
#				statusMessage = "The Staging DB has been updated accordingly, but errors were detected when pushing changes up to the TeamDynamix app :( Please contact Jus and cross your fingers, while hoping he won't make things worse. :)"
#			end
#
#			# Send success email.
#			emailBody = %Q{
#				#{@emailStyle}
#				<p>
#				Beep. Blopp.<br /><br />
#
#				Hey #{firstName.capitalize}, I have just performed the operations specified in the file "#{File.basename(opsFile)}". #{statusMessage}<br /><br />
#
#				#{@signature}
#			}
#			sendSuccessEmail(emailBody)
#
#			status = E_SUCCESS
#		else # Failed running the graph.
#			# Move files to Rejected folder.
#			rejectFiles(filelist)
#
#			# Get last error.
#			errorMessage, errorLog = getLastError(runId)
#
#			# Send failure email.
#			sendFailureEmail("Failed performing ops during CloverETL run.<br/>Please check the log file #{@logPath}.<br/><br/>Clover Error Message:<br/><code>#{errorMessage.gsub("\n", "<br/>")}</code><br/>Clover Error Log:<br/><code>#{errorLog.gsub("\n", "<br/>")}</code><br/><br/>")
#
#			status = E_GRAPH_ERROR
#		end
	end
end

if ARGV.empty?
	puts "Usage: #{__FILE__} <app> [params]"
	puts "       #{__FILE__} list"
else
	td = TDRUBY.new
	td.send(ARGV[0], *ARGV[1..-1])
end

