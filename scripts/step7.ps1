
param(
    [string]$serverName,     # SQL Server instance name
    [string]$databaseName,    # Database name
    [string]$scriptParentFolder,   # Parent folder containing subfolders with .sql files
    [string]$logFolder,
    [string]$logTableName = "ScriptExecutionLog_2" # Name of the log table
)

$currentdate = Get-Date -Format "ddMMyyyyHHmmss"
 
# Set ErrorActionPreference to stop on all errors
$ErrorActionPreference = "Stop"
 
# Ensure the log folder exists or create it
if (-not (Test-Path $logFolder)) {
    New-Item -Path $logFolder -ItemType Directory -Force
}
 
# ---
# Helper function to log details to the database table
# ---
function Write-LogToDatabase {
    param(
        [string]$logServer,
        [string]$logDatabase,
        [string]$logTable,
        [string]$scriptName,
        [string]$status,
        [string]$message,
        [string]$objectType  # New parameter for object type
    )
 
    try {
        # Create the connection string with explicit encryption and trust settings
        $connectionString = "Server=$logServer;Database=$logDatabase;Integrated Security=True;Encrypt=True;TrustServerCertificate=True;"
 
        # Check if log table exists and create it if not
        # NOTE: Updated the CREATE TABLE query to include the new column
        $checkTableQuery = @"
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '$logTable')
BEGIN
    CREATE TABLE $logTable (
        LogID INT IDENTITY(1,1) PRIMARY KEY,
        ExecutionDate DATETIME DEFAULT GETDATE(),
        ScriptName NVARCHAR(255),
        Status NVARCHAR(50),
        ObjectType NVARCHAR(50),  -- New column
        Message NVARCHAR(MAX)
    );
END
"@
        Invoke-Sqlcmd -ConnectionString $connectionString -Query $checkTableQuery -ErrorAction Stop
 
        # Escape single quotes in the message string
        $escapedMessage = $message.Replace("'", "''")
 
        # NOTE: Updated the INSERT query to include the new ObjectType column
        $insertQuery = "INSERT INTO $logTable (ScriptName, Status, ObjectType, Message) VALUES ('$scriptName', '$status', '$objectType', '$escapedMessage')"
        Invoke-Sqlcmd -ConnectionString $connectionString -Query $insertQuery -ErrorAction Stop
 
    } catch {
        # If logging to the database fails, write the error to the console
        Write-Host "FATAL ERROR: Could not write log to the database. Error: $($_.Exception.Message)" -ForegroundColor Red
    }
}
 
# ---
# Define the order of script folders (including Functions and Triggers)
# ---
$scriptFolders = @(
    @{
        Name = "Views"
        LogFile = Join-Path -Path $logFolder -ChildPath "views_$currentdate.log"
        SuccessLog = Join-Path -Path $logFolder -ChildPath "Success_view.txt"
        FailureLog = Join-Path -Path $logFolder -ChildPath "Failure_view.txt"
        ErrorLog = Join-Path -Path $logFolder -ChildPath "Error_view.txt"
    },
    @{
        Name = "StoredProcedures"
        LogFile = Join-Path -Path $logFolder -ChildPath "storedprocedures_$currentdate.log"
        SuccessLog = Join-Path -Path $logFolder -ChildPath "Success_storedprocedure.txt"
        FailureLog = Join-Path -Path $logFolder -ChildPath "Failure_storedprocedure.txt"
        ErrorLog = Join-Path -Path $logFolder -ChildPath "Error_storedprocedure.txt"
    },
    @{
        Name = "Functions"
        LogFile = Join-Path -Path $logFolder -ChildPath "functions_$currentdate.log"
        SuccessLog = Join-Path -Path $logFolder -ChildPath "Success_function.txt"
        FailureLog = Join-Path -Path $logFolder -ChildPath "Failure_function.txt"
        ErrorLog = Join-Path -Path $logFolder -ChildPath "Error_function.txt"
    },
    @{
        Name = "Triggers"
        LogFile = Join-Path -Path $logFolder -ChildPath "triggers_$currentdate.log"
        SuccessLog = Join-Path -Path $logFolder -ChildPath "Success_trigger.txt"
        FailureLog = Join-Path -Path $logFolder -ChildPath "Failure_trigger.txt"
        ErrorLog = Join-Path -Path $logFolder -ChildPath "Error_trigger.txt"
    },
    @{
        Name = "udt"
        LogFile = Join-Path -Path $logFolder -ChildPath "udt_$currentdate.log"
        SuccessLog = Join-Path -Path $logFolder -ChildPath "Success_udt.txt"
        FailureLog = Join-Path -Path $logFolder -ChildPath "Failure_udt.txt"
        ErrorLog = Join-Path -Path $logFolder -ChildPath "Error_udt.txt"
    }
)
 
# ---
# Loop through each script folder
# ---
foreach ($folder in $scriptFolders) {
    $scriptFolder = Join-Path -Path $scriptParentFolder -ChildPath $folder.Name
 
    # Get all .sql files from the folder
    $sqlFiles = Get-ChildItem -Path $scriptFolder -Filter "*.sql"
 
    # Clear previous log files
    Clear-Content -Path $folder.SuccessLog -ErrorAction SilentlyContinue
    Clear-Content -Path $folder.FailureLog -ErrorAction SilentlyContinue
    Clear-Content -Path $folder.ErrorLog -ErrorAction SilentlyContinue
 
    # Loop through each file and execute its content on the SQL Server
    foreach ($file in $sqlFiles) {
        $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
        Write-Host "Executing script: $($file.Name)"
 
        # Assume failure until proven otherwise
        $status = "FAILURE"
        $message = "Unknown error."
 
        try {
            # Create the connection string with explicit encryption and trust settings
            $connectionString = "Server=$serverName;Database=$databaseName;Integrated Security=True;Encrypt=True;TrustServerCertificate=True;"
 
            # Execute the script with -ErrorAction Stop to make it a terminating error
            Invoke-Sqlcmd -ConnectionString $connectionString -Query (Get-Content -Path $file.FullName -Raw) -ErrorAction Stop
 
            # If we reach this point, the script was successful
            $status = "SUCCESS"
            $message = "Script executed successfully."
 
            Write-Host "Successfully executed: $($file.Name)"
            Add-Content -Path $folder.SuccessLog -Value $file.Name
 
        } catch {
            # Capture the error details
            $errorMessage = "Error executing script $($file.Name): $($_.Exception.Message)"
 
            # Append inner exception if it exists
            if ($_.Exception.InnerException) {
                $errorMessage += " Inner Exception: $($_.Exception.InnerException.Message)"
            }
            $message = $errorMessage
 
            # Write to file logs
            Add-Content -Path $folder.LogFile -Value "$timestamp - $errorMessage"
            Add-Content -Path $folder.ErrorLog -Value "$timestamp - $errorMessage"
            Add-Content -Path $folder.FailureLog -Value $file.Name
 
            # Write to console
            Write-Host "$timestamp - $errorMessage" -ForegroundColor Red
 
        } finally {
            # Log the result (success or failure) to the database regardless of the outcome
            # NOTE: Passing the folder name as the ObjectType
            Write-LogToDatabase -logServer $serverName -logDatabase $databaseName -logTable $logTableName -scriptName $file.Name -status $status -message $message -objectType $folder.Name
        }
    }
}
 
# Reset ErrorActionPreference
$ErrorActionPreference = "Continue"
 
Write-Host "All scripts executed."