
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

function Escape-SqlLiteral([string]$s) {
    if ($null -eq $s) { return "" }
    return ($s -replace "'", "''")
}

function Get-FullErrorText {
    param($err)
    $msg = ""
    if ($err.Exception) { $msg += $err.Exception.Message }
    if ($err.Exception.InnerException) { $msg += " | Inner: " + $err.Exception.InnerException.Message }
    if ($err.ErrorDetails -and $err.ErrorDetails.Message) { $msg += " | Details: " + $err.ErrorDetails.Message }
    if (-not $msg -or $msg.Trim() -eq "") { $msg = ($err | Out-String) }
    return $msg
}

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

# ✅ UPDATED: now logs into your SAME repl_log table but includes error_message
function Log-PublicationDrop {
    param (
        [string]$tableName,
        [string]$publication,
        [string]$InstanceName, # publisher (like before) OR distributor/reinit/log instance if failure happened before publication
        [string]$LogInstance,
        [string]$LogDatabase,
        [string]$LogTable,
        [string]$reinitInstanceName = $null,
        [string]$reinitPublication = $null,
        [string]$errorMessage = $null
    )

    $user = $env:USERNAME

    # Escape literals
    $t  = Escape-SqlLiteral $tableName
    $p  = Escape-SqlLiteral $publication
    $in = Escape-SqlLiteral $InstanceName
    $u  = Escape-SqlLiteral $user

    $reinitInstanceSql = if ($reinitInstanceName) { "N'$(Escape-SqlLiteral $reinitInstanceName)'" } else { "NULL" }
    $reinitPublicationSql = if ($reinitPublication) { "N'$(Escape-SqlLiteral $reinitPublication)'" } else { "NULL" }

    $errSql = "NULL"
    if ($errorMessage -and $errorMessage.Trim() -ne "") {
        $errSql = "N'$(Escape-SqlLiteral $errorMessage)'"
    }

    # status depends on error message existence
    $status = if ($errorMessage -and $errorMessage.Trim() -ne "") { "FAILED" } else { "SUCCESS" }


    $query =
@"
INSERT INTO [$LogDatabase].[dbo].[$LogTable] (
    table_name,
    publication_dropped,
    instance_name,
    [user],
    dropped_on,
    reinitialized_instance_name,
    publication_reinitialized,
    status,
    error_message
)
VALUES (
    N'$t',
    N'$p',
    N'$in',
    N'$u',
    GETDATE(),
    $reinitInstanceSql,
    $reinitPublicationSql,
    N'$status',
    $errSql
);
"@

    try {
        Invoke-Sqlcmd -ServerInstance $LogInstance -Database $LogDatabase -Query $query -TrustServerCertificate -ErrorAction Stop | Out-Null
        Write-Host "📝 Logged to DB: Publication [$publication] for table [$tableName]"
    } catch {
        Write-Warning "⚠️ Logging failed for publication [$publication] (Table: $tableName). Error: $($_.Exception.Message)"
        if ($_.Exception.InnerException) { Write-Warning "  Inner Exception: $($_.Exception.InnerException.Message)" }
    }
}

# ✅ NEW: log a validation/sql failure WITHOUT fake publication names and WITHOUT extra rows
function Log-Failure {
    param(
        [string]$TableName,
        [string]$FailingInstance,
        [string]$Message
    )
    # publication_dropped stays BLANK (like you want)
    Log-PublicationDrop -tableName $TableName -publication "" -InstanceName $FailingInstance `
        -LogInstance $LogInstance -LogDatabase $LogDatabase -LogTable $LogTable `
        -reinitInstanceName $ReinitInstance -reinitPublication $null -errorMessage $Message
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

        $replicatedDb = $replicationServer.ReplicationDatabases | Where-Object { $_.Name -ieq $publisherDB }
        if (-not $replicatedDb) {
            $msg = "Database '$publisherDB' not found on publisher '$publisherServer'."
            Write-Warning "⚠️ $msg"
            Log-Failure -TableName $articleName -FailingInstance $publisherServer -Message $msg
            return
        }

        $pub = $replicatedDb.TransPublications | Where-Object { $_.Name -ieq $publicationName }
        if (-not $pub) {
            $msg = "Publication '$publicationName' not found in '$publisherDB' on '$publisherServer'."
            Write-Warning "⚠️ $msg"
            Log-Failure -TableName $articleName -FailingInstance $publisherServer -Message $msg
            return
        }

        $matchArticle = $pub.TransArticles | Where-Object { $_.Name -ieq $articleName }
        if (-not $matchArticle) {
            $msg = "Article '$articleName' not found in publication '$publicationName' on '$publisherServer'."
            Write-Warning "⚠️ $msg"
            Log-Failure -TableName $articleName -FailingInstance $publisherServer -Message $msg
            return
        }

        $scriptOptions = [Microsoft.SqlServer.Replication.ScriptOptions]::Creation -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::SchemaOption -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::SubscriptionCreation -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::IncludeArticles -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::IncludePublisherSideSubscriptions -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::IncludeSubscriberSideSubscriptions -bor `
                            [Microsoft.SqlServer.Replication.ScriptOptions]::IncludeSQLServerAgentJobs

        Write-ToFile "/********* Script for publication: $($pub.Name) (Publisher: $($publisherServer), DB: $($publisherDB)) *********/" $scriptFile 1

        $scriptOutput = $null
        try {
            $scriptOutput = $pub.Script($scriptOptions) | Out-String
        } catch {
            $m = Get-FullErrorText $_
            Write-Warning "❌ RMO Script() failed: $m"
            Log-Failure -TableName $articleName -FailingInstance $publisherServer -Message ("RMO scripting failed: " + $m)
        }

        if ($scriptOutput -and $scriptOutput.Trim() -ne "") {
            $scriptOutput | Out-File -Append -FilePath $scriptFile
        } else {
            $msg = "Scripting returned empty output for publication '$publicationName' on '$publisherServer'."
            Write-Warning "⚠️ $msg"
            Log-Failure -TableName $articleName -FailingInstance $publisherServer -Message $msg
        }

        $snapshotCommands = @"
exec sp_addpublication_snapshot @publication = N'$publicationName';
EXEC sp_startpublication_snapshot @publication = N'$publicationName';
"@
        $snapshotCommands | Out-File -Append -FilePath $scriptFile

    } catch {
        $m = Get-FullErrorText $_
        Write-Warning "⚠️ Unexpected scripting error: $m"
        Log-Failure -TableName $articleName -FailingInstance $publisherServer -Message ("Unexpected scripting error: " + $m)
    }
}

function Drop-PushSubscriptionFromPublisher {
    param($publisherInstance, $publisherDB, $publication, $subscriber, $subscriberDB)
    Write-Host "Attempting to drop push subscription from publisher for publication '$publication' (Publisher: $publisherInstance, Subscriber: $subscriber)..."
    $query = "EXEC sp_dropsubscription @publication = N'$publication', @subscriber = N'$subscriber', @article = N'all'"
    try {
        Invoke-Sqlcmd -ServerInstance $publisherInstance -Database $publisherDB -Query $query -TrustServerCertificate -ErrorAction Stop
        Write-Host "✅ Successfully dropped push subscription from publisher for publication '$publication'."
        return [pscustomobject]@{ success = $true; error = $null }
    } catch {
        $msg = Get-FullErrorText $_
        Write-Warning "⚠️ Failed to drop push subscription from publisher for publication '$publication'. Error: $msg"
        return [pscustomobject]@{ success = $false; error = $msg }
    }
}

function Clean-UpSubscriptionMetadataOnSubscriber {
    param($subscriberInstance, $subscriberDB, $publisher, $publisherDB, $publication)
    Write-Host "Attempting to clean up subscription metadata on subscriber '$subscriberInstance' for publication '$publication'..."
    $query = "EXEC sp_subscription_cleanup @publisher = N'$publisher', @publisher_db = N'$publisherDB', @publication = N'$publication'"
    try {
        Invoke-Sqlcmd -ServerInstance $subscriberInstance -Database $subscriberDB -Query $query -TrustServerCertificate -ErrorAction Stop
        Write-Host "✅ Successfully cleaned up subscription metadata on subscriber for publication '$publication'."
        return [pscustomobject]@{ success = $true; error = $null }
    } catch {
        $msg = Get-FullErrorText $_
        Write-Warning "⚠️ Failed to clean up subscription metadata on subscriber for publication '$publication'. Error: $msg"
        return [pscustomobject]@{ success = $false; error = $msg }
    }
}

function Drop-PublicationFromPublisher {
    param($publisher, $publisherDB, $publication)
    Write-Host "Attempting to drop publication '$publication' from publisher '$publisher'..."
    try {
        try {
            Invoke-Sqlcmd -ServerInstance $publisher -Database $publisherDB -Query "EXEC sp_dropsubscription @publication = N'$publication', @article = N'all', @subscriber = N'all'" -TrustServerCertificate -ErrorAction Stop
        } catch {
            $subMsg = Get-FullErrorText $_
            Write-Warning "⚠️ Failed to drop all subscriptions for '$publication'. Error: $subMsg"
            # don’t stop; capture error in final log
        }

        Invoke-Sqlcmd -ServerInstance $publisher -Database $publisherDB -Query "EXEC sp_droppublication @publication = N'$publication'" -TrustServerCertificate -ErrorAction Stop
        Write-Host "✅ Successfully dropped publication '$publication' from publisher '$publisher'."
        return [pscustomobject]@{ success = $true; error = $null }
    } catch {
        $msg = Get-FullErrorText $_
        Write-Warning "⚠️ Failed to drop publication '$publication' from publisher '$publisher'. Error: $msg"
        return [pscustomobject]@{ success = $false; error = $msg }
    }
}

function Reinitialize-Replication {
    param($server, $tableName)
    $reinitPublications = @()
    Write-Host "Instantiating ReplicationServer object for reinitialization: $server..."
    try {
        $replicationServer = New-Object "Microsoft.SqlServer.Replication.ReplicationServer" $server
    } catch {
        $m = Get-FullErrorText $_
        Write-Warning "⚠️ Failed to instantiate ReplicationServer for reinit '$server'. Error: $m"
        Log-Failure -TableName $tableName -FailingInstance $server -Message ("Reinit connect failed: " + $m)
        return $reinitPublications
    }

    try {
        foreach ($replicatedDb in $replicationServer.ReplicationDatabases) {
            foreach ($pub in $replicatedDb.TransPublications) {
                $match = $pub.TransArticles | Where-Object { $_.Name -ieq $tableName }
                if ($match) {
                    if ($pub.TransSubscriptions.Count -gt 0) {
                        foreach ($sub in $pub.TransSubscriptions) {
                            try {
                                $sub.Reinitialize()
                            } catch {
                                $m = Get-FullErrorText $_
                                Write-Warning "⚠️ Failed reinit for publication '$($pub.Name)'. Error: $m"
                                Log-PublicationDrop -tableName $tableName -publication $pub.Name -InstanceName $server `
                                    -LogInstance $LogInstance -LogDatabase $LogDatabase -LogTable $LogTable `
                                    -reinitInstanceName $server -reinitPublication $pub.Name -errorMessage ("Reinit failed: " + $m)
                            }
                        }
                        $reinitPublications += $pub.Name
                    }
                }
            }
        }
    } catch {
        $m = Get-FullErrorText $_
        Write-Warning "⚠️ Reinit traverse failed on '$server'. Error: $m"
        Log-Failure -TableName $tableName -FailingInstance $server -Message ("Reinit traverse failed: " + $m)
    }

    return $reinitPublications
}

#endregion

# --- Main Script Execution ---
Write-Host "Starting replication script execution at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')..."

# Validate table file path (log + exit)
if (-not (Test-Path $TablesFilePath)) {
    $msg = "Tables file not found at '$TablesFilePath'."
    Write-Error $msg
    Log-Failure -TableName "__GLOBAL__" -FailingInstance $env:COMPUTERNAME -Message $msg
    exit 1
}

$tables = Get-Content $TablesFilePath | Where-Object { $_.Trim() -ne '' }
if ($tables.Count -eq 0) {
    $msg = "Tables file '$TablesFilePath' is empty (no tables)."
    Write-Warning $msg
    Log-Failure -TableName "__GLOBAL__" -FailingInstance $env:COMPUTERNAME -Message $msg
    exit 0
}

Emit-Progress 15 "TABLE_LIST_READY"

# Ensure output path exists
if (-not (Test-Path $OutputPath)) {
    Write-Host "Output path '$OutputPath' does not exist. Creating it now."
    try {
        New-Item -ItemType Directory -Force -Path $OutputPath | Out-Null
        Write-Host "✅ Output path created: '$OutputPath'."
    } catch {
        $m = Get-FullErrorText $_
        $msg = "Failed to create output path '$OutputPath' | $m"
        Write-Error $msg
        Log-Failure -TableName "__GLOBAL__" -FailingInstance $env:COMPUTERNAME -Message $msg
        exit 1
    }
}
Emit-Progress 20 "OUTPUT_PATH_READY"

$totalTables = $tables.Count
$idx = 0

foreach ($TableName in $tables) {

    $TableName = $TableName.Trim()
    if ([string]::IsNullOrWhiteSpace($TableName)) { continue }

    Write-Host "`n--- Processing table: $TableName ---"
    $idx++

    $base = 20
    $span = 75
    $pct = $base + [math]::Floor(($idx / [math]::Max(1, $totalTables)) * $span)
    Emit-Progress $pct ("PROCESSING_TABLE {0}/{1}: {2}" -f $idx, $totalTables, $TableName)

    $timestamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $scriptFile = Join-Path $OutputPath "$TableName-$timestamp.sql"
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

    $replicationInfo = $null
    try {
        $replicationInfo = Invoke-Sqlcmd -ServerInstance $InstanceName -Database "distribution" -Query $replicationQuery -TrustServerCertificate -ErrorAction Stop
        Emit-Progress $pct ("DISTRIBUTION_QUERY_OK: {0}" -f $TableName)
    } catch {
        $m = Get-FullErrorText $_
        $msg = "Distributor query failed for table '$TableName' on '$InstanceName' | $m"
        Write-Warning $msg
        # ✅ single row, no fake publication
        Log-Failure -TableName $TableName -FailingInstance $InstanceName -Message $msg
        continue
    }

    if (-not $replicationInfo -or $replicationInfo.Count -eq 0) {
        $msg = "No active push replication found for table '$TableName' in distribution DB on '$InstanceName'."
        Write-Host "❌ $msg"
        # ✅ single row, no fake publication, instance = distributor
        Log-Failure -TableName $TableName -FailingInstance $InstanceName -Message $msg
        continue
    }

    # --- Reinitialization Phase ---
    $reinitPublications = @()
    if ($ReinitInstance) {
        $reinitPublications = Reinitialize-Replication -server $ReinitInstance -tableName $TableName
    }

    Emit-Progress $pct ("REINIT_PHASE_DONE: {0}" -f $TableName)

    foreach ($row in $replicationInfo) {

        $publication  = $row.publication
        $publisher    = $row.Source_Server
        $publisherDB  = $row.publisher_db
        $subscriber   = $row.Destination_Server
        $subscriberDB = $row.subscriber_db

        Write-Host "`n--- Handling publication '$publication' (Publisher: $publisher, Subscriber: $subscriber) ---"

        # Scripting (same as before)
        if (-not ($reinitPublications -contains $publication)) {
            Script-SpecificPublication -publisherServer $publisher -publisherDB $publisherDB -publicationName $publication -articleName $TableName -scriptFile $scriptFile
        }

        # Skip drop if publisher == reinitinstance (same as before)
        if ($ReinitInstance -and ($publisher.ToLower() -eq $ReinitInstance.ToLower())) {
            $reinitPubFound = $reinitPublications | Where-Object { $_ -eq $publication } | Select-Object -First 1
            Log-PublicationDrop -tableName $TableName -publication $publication -InstanceName $publisher `
                -LogInstance $LogInstance -LogDatabase $LogDatabase -LogTable $LogTable `
                -reinitInstanceName $ReinitInstance -reinitPublication $reinitPubFound -errorMessage $null
            continue
        }

        # Drop operations (same) but now we capture ALL errors into ONE final error_message
        $errs = @()

        $r1 = Drop-PushSubscriptionFromPublisher -publisherInstance $publisher -publisherDB $publisherDB -publication $publication -subscriber $subscriber -subscriberDB $subscriberDB
        if ($r1 -and (-not $r1.success) -and $r1.error) { $errs += ("PublisherDropSub: " + $r1.error) }

        $r2 = Clean-UpSubscriptionMetadataOnSubscriber -subscriberInstance $subscriber -subscriberDB $subscriberDB -publisher $publisher -publisherDB $publisherDB -publication $publication
        if ($r2 -and (-not $r2.success) -and $r2.error) { $errs += ("SubscriberCleanup: " + $r2.error) }

        $r3 = Drop-PublicationFromPublisher -publisher $publisher -publisherDB $publisherDB -publication $publication
        if ($r3 -and (-not $r3.success) -and $r3.error) { $errs += ("DropPublication: " + $r3.error) }

        $finalErr = $null
        if ($errs.Count -gt 0) { $finalErr = ($errs -join " || ") }

        $reinitPubFound = $reinitPublications | Where-Object { $_ -eq $publication } | Select-Object -First 1

        # ✅ EXACTLY like your old script:
        # publication = real publication
        # instance_name = publisher
        # reinit_instance = ReinitInstance
        # error_message = aggregated SQL + validation failures
        Log-PublicationDrop -tableName $TableName -publication $publication -InstanceName $publisher `
            -LogInstance $LogInstance -LogDatabase $LogDatabase -LogTable $LogTable `
            -reinitInstanceName $ReinitInstance -reinitPublication $reinitPubFound -errorMessage $finalErr
    }
}

Emit-Progress 100 "END"
Write-Host "`n--- Script execution complete at $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss') ---"
