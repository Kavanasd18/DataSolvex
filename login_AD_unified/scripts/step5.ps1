param (
    [string]$serverName,
    [string]$databaseName,
    [string]$outputRootFolder  
)

# Load the SMO assembly
if (Get-Module -ListAvailable -Name SqlServer) {
    Import-Module SqlServer
} else {
    Write-Host "SqlServer module is not available."
    exit
}

# SQL Server connection details
$viewsFolder = "Views"
$functionsFolder = "Functions"
$storedProceduresFolder = "StoredProcedures"
$triggersFolder = "Triggers"

# Ensure output folders exist
$viewsFolderPath = Join-Path -Path $outputRootFolder -ChildPath $viewsFolder
$functionsFolderPath = Join-Path -Path $outputRootFolder -ChildPath $functionsFolder
$storedProceduresFolderPath = Join-Path -Path $outputRootFolder -ChildPath $storedProceduresFolder
$triggersFolderPath = Join-Path -Path $outputRootFolder -ChildPath $triggersFolder
$foldersToCreate = @($viewsFolderPath, $functionsFolderPath, $storedProceduresFolderPath, $triggersFolderPath)

foreach ($folder in $foldersToCreate) {
    if (-Not (Test-Path -Path $folder)) {
        New-Item -Path $folder -ItemType Directory
        Write-Host "Created folder: $folder"
    } else {
        Write-Host "Folder already exists: $folder"
    }
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

# Get object definitions with error handling for each block
foreach ($sp in $database.StoredProcedures) {
    try {
        if ($sp.IsSystemObject -eq $false) {
            $schema = $sp.Schema
            $name = $sp.Name
            $definition = $sp.Script() -join "`r`n"
            Save-ObjectDefinition -folder $storedProceduresFolderPath -schema $schema -name $name -type "Stored Procedure" -definition $definition
        }
    } catch {
        Write-Host "Error processing Stored Procedure $($sp.Name): $($_.Exception.Message)"
    }
}

foreach ($view in $database.Views) {
    try {
        if ($view.IsSystemObject -eq $false) {
            $schema = $view.Schema
            $name = $view.Name
            $definition = $view.Script() -join "`r`n"
            Save-ObjectDefinition -folder $viewsFolderPath -schema $schema -name $name -type "View" -definition $definition
        }
    } catch {
        Write-Host "Error processing View $($view.Name): $($_.Exception.Message)"
    }
}

foreach ($function in $database.UserDefinedFunctions) {
    try {
        if ($function.IsSystemObject -eq $false) {
            $schema = $function.Schema
            $name = $function.Name
            $definition = $function.Script() -join "`r`n"
            Save-ObjectDefinition -folder $functionsFolderPath -schema $schema -name $name -type "Function" -definition $definition
        }
    } catch {
        Write-Host "Error processing Function $($function.Name): $($_.Exception.Message)"
    }
}

foreach ($table in $database.Tables) {
    try {
        if ($table.IsSystemObject -eq $false) {
            foreach ($trigger in $table.Triggers) {
                try {
                    if ($trigger.IsSystemObject -eq $false) {
                        $schema = $trigger.Schema
                        $name = $trigger.Name
                        $definition = $trigger.Script() -join "`r`n"

                        # Remove ALTER TABLE ENABLE TRIGGER line
                        $triggerAlterPattern = 'ALTER TABLE \[.*\] ENABLE TRIGGER \[.*\]'
                        $definition = $definition -replace $triggerAlterPattern, ""

                        Save-ObjectDefinition -folder $triggersFolderPath -schema $schema -name $name -type "Trigger" -definition $definition
                    }
                } catch {
                    Write-Host "Error processing Trigger $($trigger.Name) on Table $($table.Name): $($_.Exception.Message)"
                }
            }
        }
    } catch {
        Write-Host "Error processing Table $($table.Name): $($_.Exception.Message)"
    }
}

# Output the total number of files created
Write-Host "Total files created: $fileCount"
