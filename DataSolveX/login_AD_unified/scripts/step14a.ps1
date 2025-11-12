param (
    [string]$serverName,
    [string]$databaseName,
    [string]$outputRootFolder,
    [string]$functionsListFile # Path to the text file containing function names
)

# Check for availability of the SqlServer module (required for SMO)
if (Get-Module -ListAvailable -Name SqlServer) {
    Import-Module SqlServer
} else {
    Write-Host "SqlServer module is not available."
    exit
}

# SQL Server connection details
$functionsFolder = "Functions"

# Ensure output folders exist
$functionsFolderPath = Join-Path -Path $outputRootFolder -ChildPath $functionsFolder

if (-Not (Test-Path -Path $functionsFolderPath)) {
    New-Item -Path $functionsFolderPath -ItemType Directory
    Write-Host "Created folder: $functionsFolderPath"
} else {
    Write-Host "Folder already exists: $functionsFolderPath"
}

# Connect to the SQL Server instance
try {
    $server = New-Object Microsoft.SqlServer.Management.Smo.Server($serverName)
    $database = $server.Databases[$databaseName]
    if ($database -eq $null) {
        throw "Database $databaseName not found on server $serverName."
    }
    Write-Host "Connected to database: $databaseName"
} catch {
    Write-Host "Error: $($_.Exception.Message)"
    exit
}

# Initialize the file counter
$fileCount = 0

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
    Write-Host "Saving file to: $fileName"
    if ($definition) {
        $content = "/* Object Type: ${type} */`r`n${definition}"
        $content | Out-File -FilePath $fileName -Encoding UTF8
        Write-Host "Saved ${type}: ${schema}.${name} to $fileName"
        $global:fileCount++
    } else {
        Write-Host "Definition for ${schema}.${name} is empty. Skipping."
    }
}

# Read the list of function names from the file
if (Test-Path $functionsListFile) {
    $functionNames = Get-Content -Path $functionsListFile
    Write-Host "Loaded function names from: $functionsListFile"
} else {
    Write-Host "Error: The functions list file does not exist at the specified path."
    exit
}

# Get object definitions for functions based on the list in the text file
foreach ($functionName in $functionNames) {
    # Trim any extra spaces from the function name
    $functionName = $functionName.Trim()

    try {
        # Search for the function in the database (by schema and name)
        $function = $database.UserDefinedFunctions | Where-Object { "$($_.Schema).$($_.Name)" -eq $functionName }

        if ($function) {
            $schema = $function.Schema
            $name = $function.Name
            $definition = $function.Script() -join "`r`n"
            Save-ObjectDefinition -folder $functionsFolderPath -schema $schema -name $name -type "Function" -definition $definition
        } else {
            Write-Host "Function not found: $functionName"
        }
    } catch {
        # This catch block handles errors for a specific function, allowing the script to continue
        Write-Host "Error processing function $functionName $($_.Exception.Message)"
    }
}

# Output the total number of files created
Write-Host "Total files created: $fileCount"