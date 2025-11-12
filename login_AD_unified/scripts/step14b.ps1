    # Parameters
    Param(
    [string]$serverName, # SQL Server instance
    [string]$databaseName, # Database name
    [string]$outputRootFolder, # Root folder for SQL scripts
    [string]$viewsFolder, # Folder for views
    [string]$viewsListFilePath  # Path to the text file containing view names
    )

    # Load SMO assembly (replace with the correct path for your system if needed)
    Add-Type -Path "C:\Program Files\WindowsPowerShell\Modules\SQL-SMO\0.5.0.0\Microsoft.SqlServer.Smo.dll"

    # Ensure output folder exists
    $viewsFolderPath = Join-Path -Path $outputRootFolder -ChildPath $viewsFolder

    if (-Not (Test-Path -Path $viewsFolderPath)) {
        New-Item -Path $viewsFolderPath -ItemType Directory
        Write-Host "Created folder: $viewsFolderPath"
    } else {
        Write-Host "Folder already exists: $viewsFolderPath"
    }

    # Connect to the SQL Server instance
    try {
        $server = New-Object Microsoft.SqlServer.Management.Smo.Server($serverName)
        if ($server.Databases[$databaseName] -eq $null) {
            throw "Database $databaseName not found on server $serverName. Please check the database name."
        }
        $database = $server.Databases[$databaseName]
        Write-Host "Connected to database: $databaseName"
    } catch {
        Write-Host "Error connecting to the server or database: $($_.Exception.Message)"
        exit
    }

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
            $content = "/* Object Type: View */`r`n${definition}"
            $content | Out-File -FilePath $fileName -Encoding UTF8
            Write-Host "Saved View: ${schema}.${name} to $fileName"
        } else {
            Write-Host "Definition for ${schema}.${name} is empty. Skipping."
        }
    }

    # Read the list of views from the text file
    try {
        $viewsList = Get-Content -Path $viewsListFilePath
    } catch {
        Write-Host "Error reading views list file: $($_.Exception.Message)"
        exit
    }

    # Loop through views to get their definitions, skipping errors
    foreach ($view in $database.Views) {
        # Use a try...catch block to handle potential errors with a specific view
        try {
            if ($view.IsSystemObject -eq $false) {
                $fullName = "$($view.Schema).$($view.Name)"
                if ($viewsList -contains $fullName) {
                    $schema = $view.Schema
                    $name = $view.Name
                    # The .Script() method can throw an error, so it's inside the try block
                    $definition = $view.Script() -join "`r`n"
                    Save-ObjectDefinition -folder $viewsFolderPath -schema $schema -name $name -definition $definition
                }
            }
        } catch {
            # This catch block will execute if an error occurs for a single view
            Write-Host "Error processing view $($view.Schema).$($view.Name): $($_.Exception.Message)"
            Write-Host "Skipping to the next view."
        }
    }