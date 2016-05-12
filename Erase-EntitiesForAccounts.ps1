
$ErrorActionPreference = "Stop"

function Main {

     Write-Host "Started erase entities at $(Get-Date)..."

    $accounts = [string[]] ("test", "test2")
    $tableNames = [string[]] ("tableName1")

    if((Get-Module -ListAvailable Azure) -eq $null) {
        Write-Warning "Windows Azure PowerShell module not found! Please install from http://www.windowsazure.com/en-us/downloads/#cmd-line-tools"
        exit 1
    }
        
    $jobs = @()    

    foreach($account in $accounts) {
        foreach($tableName in $tableNames) {
            [string[]]$singleAccountList = $account
            [string[]]$singleTableList = $tableName        
            $jobs += Start-Job -FilePath .\Erase-TableEntities.ps1 -ArgumentList "azure.subscription.name", $singleAccountList, $singleTableList, 30000, 50000
        }
    }

    $jobs | Get-Job | Wait-Job | Receive-Job 
    $jobs | Remove-Job 

    Write-Host "Erase completed at $(Get-Date)..."
}

Main

exit 0