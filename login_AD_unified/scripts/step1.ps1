param(
    [string]$serverName,
    [string]$databaseName,
    [string]$outputRootFolder,
    [string]$viewsFolder,
    [string]$outputFile,
    [string]$viewNamesFile
)

# Ensure that the SqlServer module is loaded
Import-Module SqlServer -Force


# Define the connection string (to bypass certificate validation)
$connectionString = "Server=$serverName;Database=$databaseName;Trusted_Connection=True;TrustServerCertificate=True;"
$query = "SELECT name FROM sys.views WHERE OBJECTPROPERTY(object_id, 'IsSchemaBound') = 1"


# Run the query, select only the 'name' column, and store the results in the text file without headers
Invoke-Sqlcmd -ConnectionString $connectionString -Query $query | ForEach-Object { $_.name } | Out-File -FilePath $outputFile -Encoding UTF8

# Ensure output folder exists
$viewsFolderPath = Join-Path -Path $outputRootFolder -ChildPath $viewsFolder

# Create the views folder if it doesn't exist
if (-Not (Test-Path -Path $viewsFolderPath)) {
    New-Item -Path $viewsFolderPath -ItemType Directory
    Write-Host "Created folder: $viewsFolderPath"
} else {
    Write-Host "Folder already exists: $viewsFolderPath"
}

# Connect to the SQL Server instance using SMO
$server = New-Object Microsoft.SqlServer.Management.Smo.Server($serverName)

# Ensure connection to database is valid
if ($server.Databases[$databaseName] -eq $null) {
    Write-Host "Database $databaseName not found on server $serverName. Please check the database name."
    exit
}

# Get the database
$database = $server.Databases[$databaseName]
Write-Host "Connected to database: $databaseName"

# Read view names from the text file
$viewNames = Get-Content -Path $viewNamesFile | Where-Object { $_.Trim() -ne "" }

# Function to save object definition to a file
function Save-ObjectDefinition {
    param(
        [string]$folder,
        [string]$schema,
        [string]$name,
        [string]$type,
        [string]$definition
    )
    $fileName = Join-Path -Path $folder -ChildPath "$($schema)_$($name).sql"
    # Log the file path for clarity
    Write-Host "Saving file to: $fileName"
    
    # Check if the definition exists
    if ($definition) {
        # Replace WITH SCHEMABINDING with an empty string
        $definition = $definition -replace "WITH SCHEMABINDING", ""
        
        # Replace CREATE VIEW with ALTER VIEW
        $definition = $definition -replace "CREATE VIEW", "ALTER VIEW"
        
        # Replace SET QUOTED_IDENTIFIER ON or SET QUOTED_IDENTIFIER OFF with GO
        $definition = $definition -replace "SET QUOTED_IDENTIFIER (ON|OFF)", "GO"
        
        # Write the modified definition to the file
        $content = "/* Object Type: ${type} */`r`n${definition}"
        $content | Out-File -FilePath $fileName -Encoding UTF8
        Write-Host "Saved ${type}: ${schema}.${name} to $fileName"
    } else {
        Write-Host "Definition for ${schema}.${name} is empty. Skipping."
    }
}

# Loop through views to get their definitions and save to files
foreach ($view in $database.Views) {
    if ($view.IsSystemObject -eq $false -and $viewNames -contains $view.Name) {
        $schema = $view.Schema
        $name = $view.Name
        $definition = $view.Script() -join "`r`n"
        Save-ObjectDefinition -folder $viewsFolderPath -schema $schema -name $name -type "View" -definition $definition
    }
}

# Execute the generated SQL files (view definitions) against the same server
$generatedViewFiles = Get-ChildItem -Path $viewsFolderPath -Filter "*.sql"

foreach ($file in $generatedViewFiles) {
    Write-Host "Executing script: $($file.FullName)"
    
    # Read the content of the SQL script
    $scriptContent = Get-Content -Path $file.FullName -Raw
    
    # Execute the script against the server
    try {
        Invoke-Sqlcmd -ConnectionString $connectionString -Query $scriptContent
        Write-Host "Successfully executed: $($file.Name)"
    } catch {
        Write-Host "Failed to execute: $($file.Name). Error: $_"
    }
}