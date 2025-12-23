param (

    [Parameter(Mandatory = $true)]
    [string]$InstanceName,
 
    [Parameter(Mandatory = $true)]

    [ValidateScript({ Test-Path $_ })]

    [string]$SqlFilePath,
 
    [Parameter(Mandatory = $true)]

    [string]$DatabaseName,
 
    # ---- logging target ----

    [Parameter(Mandatory = $true)]

    [string]$ReinitLogInstance,
 
    [Parameter(Mandatory = $true)]

    [string]$ReinitLogDatabase,
 
    [Parameter(Mandatory = $true)]

    [string]$ReinitLogTable,
 
    # ---- log payload ----

    [Parameter(Mandatory = $true)]

    [string]$ReinitializeInstance,
 
    [Parameter(Mandatory = $true)]

    [string]$PublicationName,
 
    # Optional now (allow empty)

    [Parameter(Mandatory = $false)]

    [string]$ExecutedBy = "",
 
    [Parameter(Mandatory = $true)]

    [string]$InitiatedTime

)
 
$ErrorActionPreference = "Stop"

Import-Module SQLServer -ErrorAction Stop
 
function Escape-SqlLiteral([string]$s) {

    if ($null -eq $s) { return "" }

    return $s.Replace("'", "''")

}
 
function Insert-RunningRow {

    $ri  = Escape-SqlLiteral $ReinitializeInstance

    $pub = Escape-SqlLiteral $PublicationName

    $exe = Escape-SqlLiteral $ExecutedBy

    $ini = Escape-SqlLiteral $InitiatedTime

    $pth = Escape-SqlLiteral $SqlFilePath
 
    $q = @"

SET NOCOUNT ON;
 
INSERT INTO dbo.$ReinitLogTable

(

    Reinitalize_instance,

    publication_name,

    executed_by,

    initiated_time,

    status,

    sql_file_path

)

VALUES

(

    N'$ri',

    N'$pub',

    NULLIF(N'$exe', N''),

    CONVERT(DATETIME2(0), N'$ini', 120),

    N'RUNNING',

    N'$pth'

);
 
SELECT CAST(SCOPE_IDENTITY() AS INT) AS id;

"@
 
    $row = Invoke-Sqlcmd -ServerInstance $ReinitLogInstance -Database $ReinitLogDatabase -Query $q -TrustServerCertificate -ErrorAction Stop

    return [int]$row.id

}
 
function Update-Row([int]$Id, [string]$Status, [string]$ErrorMessage = "") {

    $st  = Escape-SqlLiteral $Status

    $err = Escape-SqlLiteral $ErrorMessage
 
    $q = @"

SET NOCOUNT ON;
 
UPDATE dbo.$ReinitLogTable

SET

    status = N'$st',

    error_message = NULLIF(N'$err', N'')

WHERE id = $Id;

"@
 
    Invoke-Sqlcmd -ServerInstance $ReinitLogInstance -Database $ReinitLogDatabase -Query $q -TrustServerCertificate -ErrorAction Stop | Out-Null

}
 
# -----------------------------

# Main

# -----------------------------

$rowId = $null
 
try {

    Write-Host "📝 Logging RUNNING to [$ReinitLogInstance].[$ReinitLogDatabase].dbo.[$ReinitLogTable]..."

    $rowId = Insert-RunningRow

    Write-Host "🧾 Log RowId: $rowId"
 
    # 🔥 KEEP ORIGINAL BEHAVIOR: execute file exactly the same

    Write-Host "🚀 Executing script [$SqlFilePath] on [$InstanceName], Database: [$DatabaseName]..."

    Invoke-Sqlcmd -ServerInstance $InstanceName -Database $DatabaseName -InputFile $SqlFilePath -TrustServerCertificate -ErrorAction Stop

    Write-Host "✅ Script executed successfully."
 
    if ($rowId -ne $null) {

        Update-Row -Id $rowId -Status "SUCCESS"

    }
 
    exit 0

}

catch {

    $msg = $_.Exception.Message

    Write-Host "❌ Failed to execute script: $msg"
 
    # include deeper info when available

    $detail = $msg

    if ($_.Exception.InnerException) {

        $detail = $detail + " | Inner: " + $_.Exception.InnerException.Message

    }
 
    try {

        if ($rowId -ne $null) {

            Update-Row -Id $rowId -Status "FAILED" -ErrorMessage $detail

        }

    } catch {

        Write-Host "⚠️ Logging FAILED update also failed: $($_.Exception.Message)"

    }
 
    exit 1

}

 