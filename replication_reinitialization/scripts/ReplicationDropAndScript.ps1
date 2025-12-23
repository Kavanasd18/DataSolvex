# Parameters
param (
    [string]$InstanceName , # Dist name
    [string]$OutputPath , #to save the file
    [string]$TablesFilePath , #txt path
    [string]$ReinitInstance ,
    [string]$LogInstance ,
    [string]$LogDatabase ,
    [string]$LogTable
)
 
$ErrorActionPreference = 'Stop'
 
function Emit-Progress {
    param(
        [int]$Percent,
        [string]$Stage
    )
    # IMPORTANT: Write-Output so Python SSE can capture it
    Write-Output ("__PROGRESS__:{0}:{1}" -f $Percent, $Stage)
}
 
Emit-Progress 5 "START"
 
 
#region Module and Assembly Loading
Write-Host "Loading SQLServer module..."
try {
    Import-Module SQLServer -ErrorAction Stop
    Write-Host "✅ SQLServer module loaded successfully."
} catch {
    Write-Error "❌ Failed to load SQLServer module. Please ensure it's installed. Error: $($_.Exception.Message)"
    exit 1
}
 
Write-Host "Loading Microsoft.SqlServer.Replication assembly..."
try {
    [reflection.assembly]::LoadWithPartialName("Microsoft.SqlServer.Replication") | Out-Null
    Write-Host "✅ Microsoft.SqlServer.Replication assembly loaded."
} catch {
    Write-Error "❌ Failed to load Microsoft.SqlServer.Replication assembly. Error: $($_.Exception.Message)"
    exit 1
}
 
Write-Host "Loading Microsoft.SqlServer.Rmo assembly..."
try {
    [reflection.assembly]::LoadWithPartialName("Microsoft.SqlServer.Rmo") | Out-Null
    Write-Host "✅ Microsoft.SqlServer.Rmo assembly loaded."
} catch {
    Write-Error "❌ Failed to load Microsoft.SqlServer.Rmo assembly. Error: $($_.Exception.Message)"
    exit 1
}
#endregion
 
Emit-Progress 10 "MODULES_LOADED"
 
#region Helper Functions
 
function Write-ToFile {
    param([string]$text, [string]$file, [int]$newLine = 0)
    if ($newLine -eq 1) { "" | Out-File -Append -FilePath $file }
    $text | Out-File -Append -FilePath $file
    Write-Host "📜 Logged to file '$file': '$text'"
}
 
function Initialize-File {
    param([string]$file)
    "" | Out-File $file
    Write-Host "🔄 Initialized file: $file. It is now empty."
}
 
function Log-PublicationDrop {
    param (
        [string]$tableName,
        [string]$publication,
        [string]$InstanceName, # Instance where the drop occurred (Publisher)
        [string]$LogInstance,
        [string]$LogDatabase,
        [string]$LogTable,
        [string]$reinitInstanceName = $null, # Instance where reinitialization was attempted
        [string]$reinitPublication = $null,  # Name of publication reinitialized
        [bool]$scriptSkipped = $false # New parameter to indicate if scripting was skipped
    )
 
    $user = $env:USERNAME
    # Corrected: Handle NULL values for SQL query
    $reinitInstanceSql = if ($reinitInstanceName) { "N'$reinitInstanceName'" } else { "NULL" }
    $reinitPublicationSql = if ($reinitPublication) { "N'$reinitPublication'" } else { "NULL" }
    $scriptSkippedSql = if ($scriptSkipped) { "1" } else { "0" } # Although this isn't used in the current query, keep for consistency if it were added later.
 
    $query =
    @"
INSERT INTO [$LogDatabase].[dbo].[$LogTable] (
    table_name,
    publication_dropped,
    instance_name,
    [user],
    dropped_on,
    reinitialized_instance_name,
    publication_reinitialized
)
VALUES (
    N'$tableName',
    N'$publication',
    N'$InstanceName',
    N'$user',
    GETDATE(),
    $reinitInstanceSql,
    $reinitPublicationSql
);
"@
 
    try {
        Write-Host "Attempting to log publication drop to database '$LogDatabase.$LogTable' on '$LogInstance'..."
        Invoke-Sqlcmd -ServerInstance $LogInstance -Database $LogDatabase -Query $query -TrustServerCertificate
        Write-Host "📝 Logged to DB: Publication [$publication] for table [$tableName]"
    } catch {
        Write-Warning "⚠️ Logging failed for publication [$publication] (Table: $tableName). Error: $($_.Exception.Message)"
        if ($_.Exception.InnerException) {
            Write-Warning "  Inner Exception: $($_.Exception.InnerException.Message)"
        }
    }
}
 
function Script-SpecificPublication {
    param(
        [string]$publisherServer,
        [string]$publisherDB,
        [string]$publicationName,
        [string]$articleName,
        [string]$scriptFile
    )
 
    Write-Host "Attempting to script publication '$publicationName' for article '$articleName' on Publisher '$publisherServer' (DB: $publisherDB)..."
    try {
        $replicationServer = New-Object "Microsoft.SqlServer.Replication.ReplicationServer" $publisherServer
        # RMO ReplicationServer objects connect implicitly on first property access.
        # This can sometimes throw if permissions are bad or server is unreachable.
 
        $replicatedDb = $replicationServer.ReplicationDatabases | Where-Object { $_.Name -ieq $publisherDB }
        if (-not $replicatedDb) {
            Write-Warning "⚠️ Database '$publisherDB' not found on publisher '$publisherServer'. Cannot script publication '$publicationName'."
            return # Exit function if database not found
        }
        Write-Host "  Found database '$publisherDB' on publisher '$publisherServer'."
 
        $pub = $replicatedDb.TransPublications | Where-Object { $_.Name -ieq $publicationName }
        if (-not $pub) {
            Write-Warning "⚠️ Publication '$publicationName' not found in database '$publisherDB' on publisher '$publisherServer'. Cannot script."
            return # Exit function if publication not found
        }
        Write-Host "  Found publication '$publicationName' in database '$publisherDB'."
 
        $matchArticle = $pub.TransArticles | Where-Object { $_.Name -ieq $articleName }
        if (-not $matchArticle) {
            Write-Warning "⚠️ Article '$articleName' not found in publication '$publicationName' on publisher '$publisherServer'. Cannot script."
            return # Exit function if article not found
        }
 
        Write-Host "  ✅ Found target publication '$publicationName' and article '$articleName' on Publisher '$publisherServer'. Preparing to script..."
 
        # ScriptOptions to include: Creation, Schema, Subscriptions (Publisher & Subscriber sides), Articles, and SQL Server Agent Jobs.
        # The 'sp_addpublication_snapshot' command will be generated as part of the SQL Server Agent Job scripting
        # when 'IncludeSQLServerAgentJobs' is used, assuming the job can be successfully retrieved by RMO.
        $scriptOptions = [Microsoft.SqlServer.Replication.ScriptOptions]::Creation -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::SchemaOption -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::SubscriptionCreation -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::IncludeArticles -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::IncludePublisherSideSubscriptions -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::IncludeSubscriberSideSubscriptions -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::IncludeSQLServerAgentJobs
 
        Write-ToFile "/********* Script for publication: $($pub.Name) (Publisher: $($publisherServer), DB: $($publisherDB)) *********/" $scriptFile 1
        Write-Host "  Calling RMO Script() method with options: '$scriptOptions'..."
 
        $scriptOutput = $null
        try {
            # Execute the Script method. This is where the internal T-SQL error occurs if permissions are lacking or objects are missing.
            $scriptOutput = $pub.Script($scriptOptions) | Out-String
            Write-Host "  RMO Script() method executed successfully (returned content may still be empty)."
        } catch {
            Write-Warning "  ❌ CRITICAL SCRIPTING ERROR: Failed to execute RMO Script() method for publication '$($pub.Name)'."
            Write-Warning "  This often happens if associated SQL Server Agent jobs are missing/inaccessible, or permissions are insufficient."
            Write-Warning "  If 'sp_addpublication_snapshot' or job creation commands are missing from the output, this is the likely cause."
            Write-Warning "  Error: $($_.Exception.Message)"
            if ($_.Exception.InnerException) {
                Write-Warning "  Inner Exception: $($_.Exception.InnerException.Message)"
            }
            $_ | Format-List * -Force | Out-String | Write-Warning # Dump all exception properties for detailed diagnosis
            # Continue without returning here, so we can still add the manual lines if desired,
            # even if RMO scripting failed to produce full output.
        }
 
        if ($scriptOutput -and ($scriptOutput.Trim() -ne "")) {
            $scriptOutput | Out-File -Append -FilePath $scriptFile
            Write-Host "  Successfully scripted content for publication '$($pub.Name)' and wrote to file."
        } else {
            Write-Warning "  ⚠️ Scripting for publication '$($pub.Name)' returned no substantial output despite no direct RMO error. File might still be blank for this publication."
        }
           
        # --- Manually add sp_addpublication_snapshot and sp_startpublication_snapshot ---
        #Write-Host "  Appending manual snapshot commands for publication '$publicationName' to '$scriptFile'..."
        $snapshotCommands = @"
 
 
exec sp_addpublication_snapshot @publication = N'$publicationName';
 
 
EXEC sp_startpublication_snapshot @publication = N'$publicationName';
 
"@
        $snapshotCommands | Out-File -Append -FilePath $scriptFile
        Write-Host "  Manual snapshot commands appended for publication '$publicationName'."
        # --- End of manual addition ---
 
        Write-Host "  Finished processing publication '$($pub.Name)'."
 
    } catch {
        Write-Warning "⚠️ An unexpected error occurred in Script-SpecificPublication for '$publicationName' on '$publisherServer'. General Error: $($_.Exception.Message)"
        if ($_.Exception.InnerException) {
            Write-Warning "  Inner Exception: $($_.Exception.InnerException.Message)"
        }
    }
}
 
function Drop-PushSubscriptionFromPublisher {
    param($publisherInstance, $publisherDB, $publication, $subscriber, $subscriberDB)
    Write-Host "Attempting to drop push subscription from publisher for publication '$publication' (Publisher: $publisherInstance, Subscriber: $subscriber)..."
    $query = "EXEC sp_dropsubscription @publication = N'$publication', @subscriber = N'$subscriber', @article = N'all'"
    try {
        Invoke-Sqlcmd -ServerInstance $publisherInstance -Database $publisherDB -Query $query -TrustServerCertificate -ErrorAction Stop
        Write-Host "✅ Successfully dropped push subscription from publisher for publication '$publication'."
    } catch {
        Write-Warning "⚠️ Failed to drop push subscription from publisher for publication '$publication'. Error: $($_.Exception.Message)"
        if ($_.Exception.InnerException) {
            Write-Warning "  Inner Exception: $($_.Exception.InnerException.Message)"
        }
    }
}
 
function Clean-UpSubscriptionMetadataOnSubscriber {
    param($subscriberInstance, $subscriberDB, $publisher, $publisherDB, $publication)
    Write-Host "Attempting to clean up subscription metadata on subscriber '$subscriberInstance' for publication '$publication'..."
    $query = "EXEC sp_subscription_cleanup @publisher = N'$publisher', @publisher_db = N'$publisherDB', @publication = N'$publication'"
    try {
        Invoke-Sqlcmd -ServerInstance $subscriberInstance -Database $subscriberDB -Query $query -TrustServerCertificate -ErrorAction Stop
        Write-Host "✅ Successfully cleaned up subscription metadata on subscriber for publication '$publication'."
    } catch {
        Write-Warning "⚠️ Failed to clean up subscription metadata on subscriber for publication '$publication'. Error: $($_.Exception.Message)"
        if ($_.Exception.InnerException) {
            Write-Warning "  Inner Exception: $($_.Exception.InnerException.Message)"
        }
    }
}
 
function Drop-PublicationFromPublisher {
    param($publisher, $publisherDB, $publication)
    Write-Host "Attempting to drop publication '$publication' from publisher '$publisher'..."
    try {
        Write-Host "  - Dropping all subscriptions for publication '$publication'..."
        # Use try/catch for individual steps if one failure shouldn't stop the others
        try {
            Invoke-Sqlcmd -ServerInstance $publisher -Database $publisherDB -Query "EXEC sp_dropsubscription @publication = N'$publication', @article = N'all', @subscriber = N'all'" -TrustServerCertificate -ErrorAction Stop
            Write-Host "  - Subscriptions dropped for publication '$publication'."
        } catch {
            Write-Warning "  ⚠️ Failed to drop all subscriptions for publication '$publication'. Error: $($_.Exception.Message)"
        }
 
        Write-Host "  - Dropping publication '$publication' itself..."
        Invoke-Sqlcmd -ServerInstance $publisher -Database $publisherDB -Query "EXEC sp_droppublication @publication = N'$publication'" -TrustServerCertificate -ErrorAction Stop
        Write-Host "✅ Successfully dropped publication '$publication' from publisher '$publisher'."
    } catch {
        Write-Warning "⚠️ Failed to drop publication '$publication' from publisher '$publisher'. Error: $($_.Exception.Message)"
        if ($_.Exception.InnerException) {
            Write-Warning "  Inner Exception: $($_.Exception.InnerException.Message)"
        }
    }
}
 
function Reinitialize-Replication {
    param($server, $tableName)
    $reinitPublications = @()
    Write-Host "Instantiating ReplicationServer object for reinitialization: $server..."
    try {
        $replicationServer = New-Object "Microsoft.SqlServer.Replication.ReplicationServer" $server
        # RMO ReplicationServer objects connect implicitly on first property access.
        Write-Host "✅ ReplicationServer object created for reinitialization: $server."
    } catch {
        Write-Warning "⚠️ Failed to instantiate ReplicationServer for reinitialization '$server'. Error: $($_.Exception.Message)"
        return $reinitPublications # Return empty array if object creation fails
    }
 
    Write-Host "Searching for publications to reinitialize for table '$tableName' on server '$server'..."
    try {
        foreach ($replicatedDb in $replicationServer.ReplicationDatabases) {
            Write-Host "  Checking database for reinitialization: $($replicatedDb.Name)"
            foreach ($pub in $replicatedDb.TransPublications) {
                Write-Host "    - Checking publication for reinitialization: $($pub.Name)"
                $match = $pub.TransArticles | Where-Object { $_.Name -ieq $tableName }
                if ($match) {
                    Write-Host "    ✅ Found matching article for table '$tableName' in publication for reinitialization: $($pub.Name)"
                    if ($pub.TransSubscriptions.Count -gt 0) {
                        foreach ($sub in $pub.TransSubscriptions) {
                            Write-Host "    - Reinitializing subscription for publication '$($pub.Name)' (Subscriber: $($sub.SubscriberName), SubscriberDB: $($sub.SubscriptionDBName))..."
                            try {
                                $sub.Reinitialize()
                                Write-Host "    - Subscription reinitialization complete for $($pub.Name)."
                            } catch {
                                Write-Warning "    ⚠️ Failed to reinitialize subscription for publication '$($pub.Name)'. Error: $($_.Exception.Message)"
                                if ($_.Exception.InnerException) {
                                    Write-Warning "      Inner Exception: $($_.Exception.InnerException.Message)"
                                }
                            }
                        }
                        # Add the publication name to the list if it was reinitialized
                        $reinitPublications += $pub.Name
                    } else {
                        Write-Host "    - No subscriptions found for publication '$($pub.Name)' to reinitialize."
                    }
                }
            }
        }
    } catch {
        Write-Warning "⚠️ An error occurred while traversing replication objects for reinitialization on '$server'. Error: $($_.Exception.Message)"
        if ($_.Exception.InnerException) {
            Write-Warning "  Inner Exception: $($_.Exception.InnerException.Message)"
        }
    }
 
    return $reinitPublications
}
#endregion
 
# --- Main Script Execution ---
Write-Host "Starting replication script execution at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')..."
 
if (-not (Test-Path $TablesFilePath)) {
    Write-Error "Error: Tables file not found at '$TablesFilePath'. Please ensure the file exists."
    exit 1
}
 
$tables = Get-Content $TablesFilePath | Where-Object { $_.Trim() -ne '' } # Read content and filter out empty lines
if ($tables.Count -eq 0) {
    Write-Warning "The tables file '$TablesFilePath' is empty or contains only blank lines. No tables to process."
    exit 0
}
Write-Host "Tables to process from '$TablesFilePath': $($tables -join ', ')"
 
Emit-Progress 15 "TABLE_LIST_READY"
 
# Ensure output path exists
if (-not (Test-Path $OutputPath)) {
    Write-Host "Output path '$OutputPath' does not exist. Creating it now."
    try {
        New-Item -ItemType Directory -Force -Path $OutputPath | Out-Null
        Write-Host "✅ Output path created: '$OutputPath'."
    } catch {
        Write-Error "❌ Failed to create output path '$OutputPath'. Error: $($_.Exception.Message)"
        exit 1
    }
}
Emit-Progress 20 "OUTPUT_PATH_READY"
 
 
$totalTables = $tables.Count
$idx = 0
 
foreach ($TableName in $tables) {
    # Trim whitespace from table name in case there are leading/trailing spaces in the file
    $TableName = $TableName.Trim()
    if ([string]::IsNullOrWhiteSpace($TableName)) {
        Write-Host "Skipping empty table name entry."
        continue
    }
 
    Write-Host "`n--- Processing table: $TableName ---"
    $idx++
    # Map overall percent from 20% to 95% across tables
    $base = 20
    $span = 75
    $pct = $base + [math]::Floor(($idx / [math]::Max(1, $totalTables)) * $span)
    Emit-Progress $pct ("PROCESSING_TABLE {0}/{1}: {2}" -f $idx, $totalTables, $TableName)
 
    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss" # More sortable timestamp format
    $scriptFile = Join-Path $OutputPath "$TableName-$timestamp.sql"
 
    # Initialize the script file for the current table
    Initialize-File $scriptFile
 
    $replicationQuery = @"
SELECT DISTINCT
    p.publication,
    p.publisher_db,
    SUBSTRING(da.name, 0, CHARINDEX('-', da.name)) AS Source_Server,
    s.subscriber_db,
    REVERSE(SUBSTRING(REVERSE(da.name),
      CHARINDEX('-', REVERSE(da.name)) + 1,
      ((CHARINDEX('-', REVERSE(da.name),
      CHARINDEX('-', REVERSE(da.name)) + 1)) - (CHARINDEX('-', REVERSE(da.name))) - 1))) AS Destination_Server
FROM
    distribution.dbo.MSpublications p
JOIN distribution.dbo.MSarticles a
    ON a.publisher_id = p.publisher_id AND a.publication_id = p.publication_id
JOIN distribution.dbo.MSsubscriptions s
    ON s.publisher_id = p.publisher_id AND s.publication_id = p.publication_id AND s.article_id = a.article_id
JOIN distribution.dbo.MSdistribution_agents da
    ON da.publisher_id = p.publisher_id AND da.publisher_db = p.publisher_db AND da.subscriber_db = s.subscriber_db
WHERE a.article = N'$TableName' AND s.subscriber_db <> 'virtual'
"@
 
    Write-Host "Connecting to distributor '$InstanceName' and querying for replication information for table '$TableName'..."
    $replicationInfo = $null
    try {
        $replicationInfo = Invoke-Sqlcmd -ServerInstance $InstanceName -Database "distribution" -Query $replicationQuery -TrustServerCertificate -ErrorAction Stop
        Write-Host "✅ Successfully queried replication information from '$InstanceName'."
        Emit-Progress $pct ("DISTRIBUTION_QUERY_OK: {0}" -f $TableName)
    } catch {
        Write-Warning "⚠️ Failed to query replication for table '$TableName' on distributor '$InstanceName'. Error: $($_.Exception.Message)"
        if ($_.Exception.InnerException) {
            Write-Warning "  Inner Exception: $($_.Exception.InnerException.Message)"
        }
        Write-Warning "Skipping table '$TableName'."
        continue # Skip to the next table if query fails
    }
 
    if (-not $replicationInfo -or $replicationInfo.Count -eq 0) {
        Write-Host "❌ No active push replication found for table '$TableName' in distribution database. Moving to the next table."
        continue
    } else {
        Write-Host "Found replication for table '$TableName'. Details:"
        $replicationInfo | Format-Table -AutoSize | Out-String | Write-Host
    }
 
    # --- Reinitialization Phase (happens before any drops) ---
    # This array will now hold the names of publications that were reinitialized.
    $reinitPublications = @()
    if ($ReinitInstance) {
        Write-Host "Reinitialization instance '$ReinitInstance' is specified. Attempting to reinitialize replication for table '$TableName'..."
        $reinitPublications = Reinitialize-Replication -server $ReinitInstance -tableName $TableName
        if ($reinitPublications.Count -gt 0) {
            Write-Host "Reinitialized publications for table '$TableName': $($reinitPublications -join ', ')"
        } else {
            Write-Host "No publications were reinitialized for table '$TableName'."
        }
    } else {
        Write-Host "No reinitialization instance specified ($ReinitInstance is empty). Skipping reinitialization."
    }
 
    Write-Host "Processing each replication entry for table '$TableName' to perform scripting and drop operations..."
    Emit-Progress $pct ("REINIT_PHASE_DONE: {0}" -f $TableName)
 
    foreach ($row in $replicationInfo) {
        $publication  = $row.publication
        $publisher    = $row.Source_Server
        $publisherDB  = $row.publisher_db
        $subscriber   = $row.Destination_Server
        $subscriberDB = $row.subscriber_db
        $scriptSkippedForThisPublication = $false # Flag to track if scripting is skipped
 
        Write-Host "`n--- Handling publication '$publication' (Publisher: $publisher, Subscriber: $subscriber) ---"
 
        # --- Scripting Phase ---
        # NOW: Check if the CURRENT publication name is in the list of reinitialized publications
        if ($reinitPublications -contains $publication) {
            Write-Host "➡️ Skipping scripting for publication '$publication' because it was identified as being reinitialized."
            $scriptSkippedForThisPublication = $true
            # No need to call Script-SpecificPublication if we are skipping
        } else {
            Write-Host "Scripting publication '$publication' from publisher '$publisher' for table '$TableName' to '$scriptFile'..."
            Script-SpecificPublication -publisherServer $publisher -publisherDB $publisherDB -publicationName $publication -articleName $TableName -scriptFile $scriptFile
            Write-Host "Scripting complete for publication '$publication'."
        }
 
        # --- Drop Phase ---
        # The drop logic can still be based on the server if desired, or also on the specific publication name.
        # For this request, we'll keep the drop logic based on the server as before,
        # but if you want to skip dropping only reinitialized publications, you'd change this condition too.
        if ($ReinitInstance -and ($publisher.ToLower() -eq $ReinitInstance.ToLower())) {
             Write-Host "➡️ Skipping drop operations for publication '$publication' because its publisher ('$publisher') is the specified ReinitInstance ('$ReinitInstance')."
             Write-Host "    This is to allow reinitialization to take effect without immediate drop."
            # Log this specific scenario: publication was found, no drop, reinit might have happened, scripting skipped.
            $reinitPubFound = $reinitPublications | Where-Object { $_ -eq $publication } | Select-Object -First 1
            Log-PublicationDrop -tableName $TableName -publication $publication -InstanceName $publisher `
                                -LogInstance $LogInstance -LogDatabase $LogDatabase -LogTable $LogTable `
                                -reinitInstanceName $ReinitInstance -reinitPublication $reinitPubFound `
                                -scriptSkipped $scriptSkippedForThisPublication
            continue # Skip to the next row in replicationInfo, without dropping this one
        }
 
        Write-Host "Proceeding with drop operations for publication '$publication' on publisher '$publisher'."
 
        # Drop subscription from publisher side
        Drop-PushSubscriptionFromPublisher -publisherInstance $publisher -publisherDB $publisherDB -publication $publication -subscriber $subscriber -subscriberDB $subscriberDB
 
        # Clean up metadata on subscriber side
        Clean-UpSubscriptionMetadataOnSubscriber -subscriberInstance $subscriber -subscriberDB $subscriberDB -publisher $publisher -publisherDB $publisherDB -publication $publication
 
        # Drop the publication itself
        Drop-PublicationFromPublisher -publisher $publisher -publisherDB $publisherDB -publication $publication
 
        # Log the drop action. If reinitialization happened on a *different* instance but this one was dropped,
        # reinitPublication will be null for this specific log entry.
        $reinitPubFound = $reinitPublications | Where-Object { $_ -eq $publication } | Select-Object -First 1
        Log-PublicationDrop -tableName $TableName -publication $publication -InstanceName $publisher `
                            -LogInstance $LogInstance -LogDatabase $LogDatabase -LogTable $LogTable `
                            -reinitInstanceName $ReinitInstance -reinitPublication $reinitPubFound `
                            -scriptSkipped $scriptSkippedForThisPublication
    }
}
Emit-Progress $pct ("TABLE_DONE: {0}" -f $TableName)
 
Write-Host "`n--- Script execution complete at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ---"
Emit-Progress 100 "END"
 
 