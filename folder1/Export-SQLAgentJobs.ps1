<#
	.SYNOPSIS
		Export-SQLAgentJobs.ps1
	.DESCRIPTION
		Given a server instance, export all the SQL Agent jobs matching the name pattern, into the specified output folder.
	.PARAMETER ServerInstance
		Hostname of the server instance.
	.PARAMETER NamePattern
		Regular expression to match the job names to be exported. Defaults to '.*'.
	.PARAMETER OutputFolder
		Root folder where the job files will be saved. Defaults to './SQLAgentJobs'. It will follow this pattern: <OutputFolder>/<ServerInstance>/Jobfile*.sql
	.PARAMETER Logfile
		Path to the log file
	.EXAMPLE
		./Generate-SQLAgentJobs.ps1 -ServerInstance db7.twu.ca -NamePattern '^Fin.*' -OutputFolder ./SQLAgentJobs
	.INPUTS

	.OUTPUTS
		SQL Agent job script files.
	.NOTES
		Author:        Juswaldy Jusman
		Creation Date: 2021-09-14
		Dependencies:  ./Logger.ps1 (logging functionality)
#>

[CmdletBinding(SupportsShouldProcess)]

param (
	[Parameter(Mandatory)] [String] $ServerInstance,
	[String] $NamePattern = '.*',
	[String] $OutputFolder = './SQLAgentJobs',
	[String] $Logfile
)

process {

	################################################################################
	try {
		$ScriptName = $($MyInvocation.MyCommand.Name)

		# Init logging.
		if (!$Logfile) {
			$Logfile = "$ScriptName." + (Get-Date -Format "yyyyMMdd_HHmmss") + ".log"
			. ./Logger.ps1
		}
		if ($PSCmdlet.ShouldProcess($ScriptName)) { $Loglevel = "INFO" } else { $Loglevel = "WHATIF" }
		Write-Log -Level $Loglevel -Message "Start $ScriptName"

		# Load Server Management Objects, then get the specified server and its jobs.
		[System.Reflection.Assembly]::LoadWithPartialName('Microsoft.SqlServer.Smo') | Out-Null
		$server = New-Object ('Microsoft.SqlServer.Management.Smo.Server') $ServerInstance
		$jobs = $server.JobServer.Jobs | Where-Object { $_.Name -match $NamePattern }
 
		# Write out each job into its own file.
		if ($jobs -ne $null) {
			$Outpath = $OutputFolder + "/" + $ServerInstance
			New-Item -ItemType Directory -Force -Path $Outpath
			foreach ( $job in $jobs ) {
				$Outfile = $Outpath + "/" + ( $job.Name -replace "/", "_" ) + ".sql"
				Write-Log -Level $Loglevel -Message "Writing $Outfile"
				$job.Script() | Out-File -Encoding ascii -filepath ( $Outfile -replace "\[", "~" -replace "\]", "~" )
			}
		}

		Write-Log -Level $Loglevel -Message "Finish $ScriptName"
		Write-Log -Level $Loglevel -Message "--------------------------------------------------------------------------------"
	}
	catch [Exception] {
		Write-Error $Error[0]
		$err = $_.Exception
		while ( $err.InnerException ) {
			$err = $err.InnerException
			Write-Output $err.Message
		}
	}
}

