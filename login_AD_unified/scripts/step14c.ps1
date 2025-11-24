
# SQL Server connection details
# Parameters
param(
[string]$serverName ,  # SQL Server instance 
[string]$databaseName ,  # Database name 
[string]$outputRootFolder ,  # Root folder containing SQL scripts
[string]$storedProceduresFolder ,  # Folder for stored procedures
[string]$spListFilePath   # Path to the text file containing stored procedure names
)

Add-Type -Path "C:\Program Files\WindowsPowerShell\Modules\SQL-SMO\0.5.0.0\Microsoft.SqlServer.Smo.dll"

# Ensure output folder exists
$storedProceduresFolderPath = Join-Path -Path $outputRootFolder -ChildPath $storedProceduresFolder
if (-Not (Test-Path -Path $storedProceduresFolderPath)) {
    New-Item -Path $storedProceduresFolderPath -ItemType Directory
    Write-Host "Created folder: $storedProceduresFolderPath"
} else {
    Write-Host "Folder already exists: $storedProceduresFolderPath"
}

# Connect to the SQL Server instance
$server = New-Object Microsoft.SqlServer.Management.Smo.Server($serverName)

# Ensure connection to database is valid
if ($server.Databases[$databaseName] -eq $null) {
    Write-Host "Database $databaseName not found on server $serverName. Please check the database name."
    exit
}

# Get the database
$database = $server.Databases[$databaseName]
Write-Host "Connected to database: $databaseName"

# Function to save object definition to a file
function Save-ObjectDefinition {
    param(
        [string]$folder,
        [string]$schema,
        [string]$name,
        [string]$definition
    )
    $fileName = Join-Path -Path $folder -ChildPath "$($schema)_$($name).sql"
    Write-Host "Saving file to: $fileName"
    if ($definition) {
        $content = "/* Object Type: Stored Procedure */`r`n${definition}"
        $content | Out-File -FilePath $fileName -Encoding UTF8
        Write-Host "Saved Stored Procedure: ${schema}.${name} to $fileName"
    } else {
        Write-Host "Definition for ${schema}.${name} is empty. Skipping."
    }
}

# Read the list of stored procedures from the text file
$spList = Get-Content -Path $spListFilePath

# Loop through stored procedures to get their definitions
foreach ($sp in $database.StoredProcedures) {
    if ($sp.IsSystemObject -eq $false) {
        $fullName = "$($sp.Schema).$($sp.Name)"
        if ($spList -contains $fullName) {
            $schema = $sp.Schema
            $name = $sp.Name
            $definition = $sp.Script() -join "`r`n"
            Save-ObjectDefinition -folder $storedProceduresFolderPath -schema $schema -name $name -definition $definition
        }
    }
}