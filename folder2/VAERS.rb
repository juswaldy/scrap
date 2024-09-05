#!/usr/local/rbenv/shims/ruby
require 'logger'
require 'csv'
require 'tiny_tds'
require './config'

class JUSRUBY
	def initialize
		@scriptpath = "/home/jus/bin"
		@logPath = "#{@scriptpath}/log.txt"
		@stdlog = Logger.new(STDOUT)
		@filelog = Logger.new(@logPath)
		@db = nil
		@sourcedb = { :username => DBUSER, :password => DBPWD, :host => DBHOST, :database => DATABASE, :timeout => DBTIMEOUT, :message_handler => Proc.new{|m| log(:info, m.message)} }
	end
	def log(severity, message) # Log message to stdout and file.
		@stdlog.send(severity, message)
		@filelog.send(severity, message)
	end
	def initializedb # Make sure db is initialized.
		if @db == nil
			@db = TinyTds::Client.new(@sourcedb)
		end
	end
	def runsql(sql) # Run the specified sql.
		initializedb()
		if sql.size > 0
			endReached = false
			begin
				sentinel = @db.execute(sql)
				sentinel.cancel
				endReached = true
			rescue
				log(:error, "Failed SQL: #{sql}")
				endReached = false
			ensure
				return endReached
			end
		end
	end

	def runGraph(graphPath, paramHash) # Call wget to run a clover graph.
		parameters = []
		paramHash.each { |key, value| parameters.push("param_#{key}=#{value}") }
		command = "wget --header \"X-Requested-By: jus.rb\" -a #{@logPath} -nv -O- \"#{CLOVERSERVER}/graph_run?sandbox=#{CLOVERSANDBOX}&graphID=graph/#{graphPath}&#{parameters.join('&')}\""
		log(:info, "Running #{command}")
		runId = `#{command}`
		runStatus = `wget --header \"X-Requested-By: jus.rb\" -O- "#{CLOVERSERVER}/graph_status?runID=#{runId}&returnType=STATUS_TEXT&waitForStatus=FINISHED_OK&waitTimeout=#{CLOVERTIMEOUT}"`
		log(:info, "Clover runId #{runId} status = #{runStatus}")
		return runId, runStatus
	end
	def download # Download VAERS zip.
		archiveFolder = "/archive/clover/VAERS"
		filename = "AllVAERSDataCSVS"
		ext = "zip"
		wgetlog = "/home/jus/bin/wget.log"
		cookiesfile = "/home/jus/bin/cookies.txt"
		countdown = 10

		while countdown > 0
			# Download the captcha.
			captchaFile = "#{archiveFolder}/captcha.jpg"
			`wget -U "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36" -O #{captchaFile} -a #{wgetlog} --no-check-certificate --keep-session-cookies --load-cookies=#{cookiesfile} --save-cookies=#{cookiesfile} \"https://vaers.hhs.gov/eSubDownload/captchaImage\"`

			# Read the captcha.
			verificationCode = `tesseract #{captchaFile} stdout -psm 8`.chomp.chomp
			puts verificationCode
			countdown = countdown - 1
			sleep 5

			# If got 6 characters.
			if verificationCode.size == 6
				# Download the zip and get its size.
				fileLink = "https://vaers.hhs.gov/eSubDownload/verification?fn=#{filename}.#{ext}&verificationCode=#{verificationCode}"
				vaerszip = "#{archiveFolder}/#{filename}.#{Time.now.strftime('%Y%m%d')}.#{ext}"
				`wget -U "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.71 Safari/537.36" -O #{vaerszip} -a #{wgetlog} --no-check-certificate --keep-session-cookies --load-cookies=#{cookiesfile} --save-cookies=#{cookiesfile} \"#{fileLink}\"`
				zipsize = `stat --printf=%s #{vaerszip}`.to_i

				# If zipfile is small, retry.
				if zipsize < 99999
					countdown = countdown - 1
					sleep 10
				else
					`rm #{captchaFile}`
					countdown = 0
				end
			end
		end
	end
	def Load_VAERS(version, dounzip=1) # Refresh Staging with VAERS data.
		# Unzip.
		rootfolder = "/archive/clover/VAERS"
		dirname = "#{rootfolder}/#{version}"
		zipfile = "#{rootfolder}/AllVAERSDataCSVS.#{version}.zip"
		`7za x -o#{dirname} #{zipfile}` if dounzip == 1
		`chown -R tomcat:tomcat #{rootfolder}`

		# Specify exceptions.
		exceptions = {
			2009 => {
				'DATA' => [ 
					[ ",'15min", ",15min" ]
				]
			},
			2015 => {
				'DATA' => [ 
					[ "'LOESTRAN'", "LOESTRAN" ],
					[ "'Loestran'", "Loestran" ]
				]
			},
			2017 => {
				'DATA' => [ 
					[ "'Negative'", "Negative" ]
				]
			},
			2018 => {
				'DATA' => [ 
					[ "'Light-headed' ...'heavy' arm (cannot lift to the side at all); 'warm to the touch;' achiness; general malaise.", "\"'Light-headed' ...'heavy' arm (cannot lift to the side at all); 'warm to the touch;' achiness; general malaise.\"" ]
				]
			},
			2020 => {
				'DATA' => [ 
					[ "'little' blood work/blood panel; elevated white blood count; 17 EKG; 'slightly abnormal but pretty normal'", "\"'little' blood work/blood panel; elevated white blood count; 17 EKG; 'slightly abnormal but pretty normal'\"" ]
				]
			},
			2021 => {
				'DATA' => [
					[ "'cillan'", "cillan" ],
					[ "'concussion'", "concussion" ],
					[ ",'dizzy'", ",dizzy" ],
					[ "'Fuzzy brain'", "Fuzzy brain" ],
					[ "'Shakiness'", "Shakiness" ],
					[ "'Twinges' of chest pain", "\"'Twinges' of chest pain\"" ],
					[ "'SOMETIMES PASSES OUT WITH SHOTS OR GIVING BLOOD'.", "\"'SOMETIMES PASSES OUT WITH SHOTS OR GIVING BLOOD'.\"" ],
					[ "'HAS PALPITATIONS AND CHEST PAIN ALL HER LIFE'.", "\"'HAS PALPITATIONS AND CHEST PAIN ALL HER LIFE'.\"" ],
					[ "'Covid arm' 8 days after injection", "\"'Covid arm' 8 days after injection\"" ],
					[ "'COVID Arm' - Red area at injection site that has 'hives' on the perimeter and is warm to the touch.  Itchy at times.  Sore and swollen on first day of symptoms.", "\"'COVID Arm' - Red area at injection site that has 'hives' on the perimeter and is warm to the touch.  Itchy at times.  Sore and swollen on first day of symptoms.\"" ],
					[ "'Flu shots'", "Flu shots" ]
				],
				'VAX' => [ 
					[ ",'012L20A,", ",012L20A," ],
					[ ",'ER8733,", ",ER8733," ],
					[ "'Vaccine Type (", "Vaccine Type" ]
				]
			}
		}

		# Load data.
		2021.upto(2021) do |year|
			# Clean up exceptions.
			ex = exceptions[year]
			if ex && dounzip == 1
				log(:info, "Cleaning up exceptions for #{year}")
				ex.each do |filetype, subs|
					filepath = "#{dirname}/#{year}VAERS#{filetype}.csv"
					log(:info, "Processing #{filepath}")
					if dounzip == 1
						x = File.open(filepath).read
						subs.each do |s|
							x = x.gsub(s[0], s[1])
							log(:info, "Converting #{s[0]} => #{s[1]}")
						end
						outfile = File.open(filepath, 'w')
						outfile.puts x
						outfile.close
					end
				end
			end

			# Delete rows from base tables.
			log(:info, "Deleting #{year} rows")
			start_id = CSV.foreach("#{dirname}/#{year}VAERSDATA.csv", headers:true).take(1)[0]['VAERS_ID']
			['EventSymptom', 'EventVax', 'Event'].each do |t|
				sql = "DELETE FROM Staging.VAERS_#{t} WHERE VAERS_ID >= #{start_id}"
				log(:info, sql)
				runsql(sql)
			end

			# Insert rows into base tables.
			log(:info, "Inserting #{year} rows")
			parameters = {
				"EVENT_FILE" => "#{dirname}/#{year}VAERSDATA.csv",
				"EVENTVAX_FILE" => "#{dirname}/#{year}VAERSVAX.csv",
				"EVENTSYMPTOMS_FILE" => "#{dirname}/#{year}VAERSSYMPTOMS.csv",
			}
			runId, runStatus = runGraph("VAERS.Load.grf", parameters)
		end

		# Process aggregate tables.
		log(:info, "Processing aggregate tables")
		agg_tables = {
			'Vaxtype' => [
				"IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Staging' AND TABLE_NAME = 'VAERS_Vaxtype') DROP TABLE Staging.VAERS_Vaxtype;",
				"CREATE TABLE Staging.VAERS_Vaxtype ( [Id] INT IDENTITY(1, 1), [Name] VARCHAR(MAX), PRIMARY KEY CLUSTERED (Id) );",
				"INSERT INTO Staging.VAERS_Vaxtype ([Name]) SELECT DISTINCT VAX_TYPE FROM Staging.VAERS_EventVax;"
			],
			'Symptom' => [
				"IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Staging' AND TABLE_NAME = 'VAERS_Symptom') DROP TABLE Staging.VAERS_Symptom;",
				"CREATE TABLE Staging.VAERS_Symptom ( [Id] INT IDENTITY(1, 1), [Name] VARCHAR(MAX), [Version] VARCHAR(MAX), PRIMARY KEY CLUSTERED (Id) );",
				"INSERT INTO Staging.VAERS_Symptom ([Name], [Version]) SELECT DISTINCT SYMPTOM_NAME, SYMPTOM_VERSION FROM Staging.VAERS_EventSymptom;"
			]
		}
		agg_tables.each do |t, ops|
			ops.each do |op|
				log(:info, op)
				runsql(op)
			end
		end
	end
end

if ARGV.empty?
	puts "Usage: #{__FILE__} <app> [params]"
	puts "       #{__FILE__} list"
else
	jus = JUSRUBY.new
	jus.send(ARGV[0], *ARGV[1..-1])
end

