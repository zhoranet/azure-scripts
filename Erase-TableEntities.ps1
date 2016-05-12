Param(
    # Azure subscription name
    [Parameter(Mandatory=$true)]
    [string]$AzureSubscriptionName,
    # List of storage account were entities should be removed
    [Parameter(Mandatory=$true)]
    [string[]] $StorageAccounts,     
    # List of tables where all entities will be removed
    [Parameter(Mandatory=$true)]
    [string[]] $TableNames, 
    # Number of entities retrieved in one query 
    [int] $PageSize = 10000, 
    # Maximum number of entities cached for optimal removal in batch mode
    [int] $CacheSize = 50000 
)

$ErrorActionPreference = "Stop"


function Main {

    if((Get-Module -ListAvailable Azure) -eq $null) {
        Write-Warning "Windows Azure PowerShell module not found! Please install from http://www.windowsazure.com/en-us/downloads/#cmd-line-tools"
        exit 1
    }

    $startTime = Get-Date

    Write-Host "Started at $startTime"

    Select-AzureSubscription -SubscriptionName $AzureSubscriptionName

    $removeResults = Get-Entity -StorageAccounts $StorageAccounts -TableNames $TableNames -PageSize $PageSize -MaxPages 0 | Get-Batch -CacheSize $CacheSize | Remove-Entity 
    
    $elapsedTime = New-TimeSpan $startTime $(Get-Date)
    Write-Host "Removed" $removeResults.TotalCount "entities from: [" ($StorageAccounts -join ",") "] and tables: [" ($TableNames -join ",") "] for just: $elapsedTime"

    if($removeResults.ErrorCount -gt 0) {
        Write-Host "Found" $removeResults.ErrorCount " errors" -ForegroundColor Red
    }
    
    Write-Host "Succcesfully completed" -foreground Green
}

function Get-Entity {

    Param (
        [string[]] $StorageAccounts,        
        [string[]] $TableNames,
        [int]$PageSize = 1000,
        [int]$MaxPages = 0
    )

    Begin {
        $queryCount = 0
    }

    Process {

        foreach ($storageAccount in $StorageAccounts) {

            foreach ($tableName in $TableNames) { 

                $azureTable = Get-ValidAzureTable -TableName $tableName -StorageAccount $storageAccount

                if($azureTable -ne $null -and $azureTable.CloudTable -ne $null) {                    

                    $query = New-Object Microsoft.WindowsAzure.Storage.Table.TableQuery
                    $query.SelectColumns = [string[]] ("PartitionKey", "RowKey")
                    if($PageSize -gt 0) {
                        $query.TakeCount = $PageSize    
                    }

                    # Initalize the continuation token
                    $continuationToken = $null

                    #region Execute query in a segmented fashion so later functions in the pipeline can get their work started while the query continues
                    do {
                        # Execute the query
                        $result = $azureTable.CloudTable.ExecuteQuerySegmented($query, $continuationToken, $null, $null)

                        $queryCount++
                                                
                        # Save the returned continuation token
                        $continuationToken = $result.ContinuationToken

                        $entities = $result.Results

                        # just output each entity
                        foreach ($entity in $entities)
                        {                            
                            $result = New-Object PSObject -Property @{            
                                Table = $azureTable
                                Entity = $entity
                            }
                            
                            Write-Output $result
                        }
                    }
                    # Continue until there's no continuation token provided or we exceeded number of requests (for test purposes)
                    while (($continuationToken -ne $null) -and (($MaxPages -eq 0) -or ($queryCount -lt $MaxPages))) 
                    #endregion


                }
                else {
                    Write-Host "Table '$tableName' not found in '$storageAccount' account" -ForegroundColor Red
                }
            }
        }
   }

   End {

   }
}

function Get-ValidAzureTable {

    Param
    (
        [Parameter(Mandatory=$true)]
        [String]$TableName,
        [Parameter(Mandatory=$true)]
        [String]$StorageAccount
    )
    
    Get-AzureStorageAccount -StorageAccountName $StorageAccount -ErrorVariable isExistTableError

    #Check if storage account is exist
    If($isExistStorageError.Exception -eq $null) {
        
        If($TableName -ne $null) {

            $storageKey = (Get-AzureStorageKey -StorageAccountName $StorageAccount).Primary
            $azureStorageContext = New-AzureStorageContext -StorageAccountName $StorageAccount -StorageAccountKey $StorageKey 
            $table = Get-AzureStorageTable -Name $TableName -Context $azureStorageContext -ErrorAction SilentlyContinue -ErrorVariable isExistTableError

            if(($isExistTableError.Exception -eq $null) -and ($table.CloudTable -ne $null)) {                
                return $table
            }            
        }
    }
}

function Get-Batch {

    Param (
        [parameter(ValueFromPipeline=$true)]
        [PSObject[]]$TableEntity,
        [parameter(ValueFromPipeline=$false)]
        [int]$BatchSize = 100,
        [int]$CacheSize = 10000
    )

    Begin {
        $batches = @{}
        $countKeys = @{}
        $totalCacheCount = 0
    }
    
    Process
    {       
        $key = $TableEntity.Table.Uri.AbsoluteUri + "-" + $TableEntity.Entity.PartitionKey
        
        if ($batches.ContainsKey($key) -eq $false) {
            $batches.Add($key, 
                (New-Object PSObject -Property @{ 
                    Table = $TableEntity.Table
                    Operations = (New-Object Microsoft.WindowsAzure.Storage.Table.TableBatchOperation)                    
                }))

            $countKeys.Add($key, $BatchSize)
        }

        $batch = $batches[$key]
        $batch.Operations.Add([Microsoft.WindowsAzure.Storage.Table.TableOperation]::Delete($TableEntity.Entity));
        $countKeys[$key]-- 
        $totalCacheCount++

        if (($batch.Operations.Count -ge $BatchSize) ) {
            Write-Output $batch            
                        
            $totalCacheCount = $totalCacheCount - $batch.Operations.Count
            $batches.Remove($key)
            $countKeys.Remove($key)
        }
        else {

            if($totalCacheCount -gt $CacheSize) {                

                $key = $countKeys.GetEnumerator() | sort -Property Value | select -first 1 -Property Key | %{$_. Key }           
                $batch = $batches[$key]
                Write-Output $batch

                $totalCacheCount = $totalCacheCount - $batch.Operations.Count
                $batches.Remove($key)
                $countKeys.Remove($key)
                #$batch.Operations.Clear()
            }

        }
        
        #Write-Host "batches.Count: " $batches.Count "batch.Operations.Count:" $batch.Operations.Count       
        
    }

    End {
        foreach ($batch in $batches.Values) {
            if ($batch.Operations.Count -gt 0) {                
                Write-Output $batch                
            }
        }

        $batches.Clear()        
    }
    
}

function Remove-Entity {

    Param (
        [parameter(ValueFromPipeline=$true)]
        [PSObject[]]$BatchEntity
    )

    Begin {
        $totalCount = 0
        $errorCount = 0
        $gcCounter = 0 
    }

    Process {

        $batch = (New-Object Microsoft.WindowsAzure.Storage.Table.TableBatchOperation)
        foreach($batchLine in $BatchEntity.Operations) {
            $batch.Add($batchLine)
            $totalCount++
        }        

        $delResults = $BatchEntity.Table.CloudTable.ExecuteBatch($batch);
                                        
        foreach($delResult in $delResults) {

            if($delResult.HttpStatusCode -ne 204) {
                $errorCount++
            }
        }        

        $gcCounter++
        if (($gcCounter % 200) -eq 0)
        {            
            [System.GC]::Collect()
            Write-Host "`rRemoved: $totalCount at $(Get-Date)" -NoNewline
        }        
    }
    
    End {
        $result = New-Object PSObject -Property @{ 
            TotalCount = $totalCount
            ErrorCount = $errorCount
        }
        Write-Output $result        
    }    
}

Main

exit 0