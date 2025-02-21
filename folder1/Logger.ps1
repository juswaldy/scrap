# Adapted from https://gist.github.com/barsv/85c93b599a763206f47aec150fb41ca0

# Settings.
#$logFile = "log-$(gc env:computername).log"
$Loglevels = ("DEBUG","INFO","WARN","ERROR","FATAL","WHATIF")
$logLevel = "INFO"
$logSize = 10mb # 30kb
$logCount = 10

function Write-Log {
    [CmdletBinding(SupportsShouldProcess)]
    param(
		[Parameter(Mandatory, ValueFromPipeline)] [String] $Message,
		[String] $Level = "INFO"
    )

	# Make the message
	$Stamp = (Get-Date).toString("yyyy-MM-dd HH:mm:ss:fff")
	$Line = "$Stamp $Level -- $Message"

	if ($PSCmdlet.ShouldProcess("Write-Log")) {
		if ([array]::IndexOf($Loglevels, $Level) -lt 0){
			Write-Log-Line "$Stamp ERROR -- Wrong log level parameter [$Level]"
			return
		}
		Add-Content $Logfile -Value $Line
	}

	Write-Host $Line
}

# https://gallery.technet.microsoft.com/scriptcenter/PowerShell-Script-to-Roll-a96ec7d4
function Reset-Log 
{ 
    # function checks to see if file in question is larger than the paramater specified 
    # if it is it will roll a log and delete the oldes log if there are more than x logs. 
    param([string]$fileName, [int64]$filesize = 1mb , [int] $logcount = 5) 
     
    $logRollStatus = $true 
    if(test-path $filename) 
    { 
        $file = Get-ChildItem $filename 
        if((($file).length) -ige $filesize) #this starts the log roll 
        { 
            $fileDir = $file.Directory 
            #this gets the name of the file we started with 
            $fn = $file.name
            $files = Get-ChildItem $filedir | ?{$_.name -like "$fn*"} | Sort-Object lastwritetime 
            #this gets the fullname of the file we started with 
            $filefullname = $file.fullname
            #$logcount +=1 #add one to the count as the base file is one more than the count 
            for ($i = ($files.count); $i -gt 0; $i--) 
            {  
                #[int]$fileNumber = ($f).name.Trim($file.name) #gets the current number of 
                # the file we are on 
                $files = Get-ChildItem $filedir | ?{$_.name -like "$fn*"} | Sort-Object lastwritetime 
                $operatingFile = $files | ?{($_.name).trim($fn) -eq $i} 
                if ($operatingfile) 
                 {$operatingFilenumber = ($files | ?{($_.name).trim($fn) -eq $i}).name.trim($fn)} 
                else 
                {$operatingFilenumber = $null} 
 
                if(($operatingFilenumber -eq $null) -and ($i -ne 1) -and ($i -lt $logcount)) 
                { 
                    $operatingFilenumber = $i 
                    $newfilename = "$filefullname.$operatingFilenumber" 
                    $operatingFile = $files | ?{($_.name).trim($fn) -eq ($i-1)} 
                    write-host "moving to $newfilename" 
                    move-item ($operatingFile.FullName) -Destination $newfilename -Force 
                } 
                elseif($i -ge $logcount) 
                { 
                    if($operatingFilenumber -eq $null) 
                    {  
                        $operatingFilenumber = $i - 1 
                        $operatingFile = $files | ?{($_.name).trim($fn) -eq $operatingFilenumber} 
                        
                    } 
                    write-host "deleting " ($operatingFile.FullName) 
                    remove-item ($operatingFile.FullName) -Force 
                } 
                elseif($i -eq 1) 
                { 
                    $operatingFilenumber = 1 
                    $newfilename = "$filefullname.$operatingFilenumber" 
                    write-host "moving to $newfilename" 
                    move-item $filefullname -Destination $newfilename -Force 
                } 
                else 
                { 
                    $operatingFilenumber = $i +1  
                    $newfilename = "$filefullname.$operatingFilenumber" 
                    $operatingFile = $files | ?{($_.name).trim($fn) -eq ($i-1)} 
                    write-host "moving to $newfilename" 
                    move-item ($operatingFile.FullName) -Destination $newfilename -Force    
                } 
            } 
          } 
         else 
         { $logRollStatus = $false} 
    } 
    else 
    { 
        $logrollStatus = $false 
    } 
    $LogRollStatus 
} 

# to null to avoid output
$Null = @(
    Reset-Log -fileName $logFile -filesize $logSize -logcount $logCount
)
