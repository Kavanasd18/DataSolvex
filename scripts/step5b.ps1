# Define SQL Server connection info
param (
    [string]$serverName,  # Replace with your SQL Server name
    [string]$databaseName,  # Replace with your DB name
    [string]$outputFolder
)

$udtFolder = Join-Path $outputFolder "udt"

# Create output folder if not exists
if (!(Test-Path -Path $outputFolder)) {
    New-Item -ItemType Directory -Path $outputFolder -Force | Out-Null
}

# Create udt subfolder if not exists
if (!(Test-Path -Path $udtFolder)) {
    New-Item -ItemType Directory -Path $udtFolder -Force | Out-Null
}

# Load SMO
Import-Module SqlServer

# Connect to server
$server = New-Object Microsoft.SqlServer.Management.Smo.Server $serverName
$db = $server.Databases[$databaseName]

# Script out User-Defined Table Types
foreach ($udtt in $db.UserDefinedTableTypes) {
    $scripter = New-Object Microsoft.SqlServer.Management.Smo.Scripter ($server)
    $scripter.Options.ScriptDrops = $false
    $scripter.Options.WithDependencies = $false
    $scripter.Options.IncludeHeaders = $true
    $scripter.Options.SchemaQualify = $true
    $scripter.Options.ToFileOnly = $true
    $scripter.Options.FileName = "$udtFolder\$($udtt.Schema)_$($udtt.Name)_TableType.sql"
    $scripter.Script($udtt)
}

# Script out User-Defined Data Types
foreach ($udt in $db.UserDefinedDataTypes) {
    $scripter = New-Object Microsoft.SqlServer.Management.Smo.Scripter ($server)
    $scripter.Options.ScriptDrops = $false
    $scripter.Options.WithDependencies = $false
    $scripter.Options.IncludeHeaders = $true
    $scripter.Options.SchemaQualify = $true
    $scripter.Options.ToFileOnly = $true
    $scripter.Options.FileName = "$udtFolder\$($udt.Schema)_$($udt.Name)_DataType.sql"
    $scripter.Script($udt)
}

Write-Host "Script generation completed. Files saved to $udtFolder"