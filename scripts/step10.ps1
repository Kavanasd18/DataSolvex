 # Define variables
param(
    [string]$sourceServer,             # Replace with your source server name
    [string]$destinationServer,        # Replace with your destination server name
    [string]$sourceDatabase,              # Replace with your source database name
    [string]$destinationDatabase,                # Replace with your destination database name
    [string]$tableNamesFile,
    [string]$errorLogFile   # Path to your text file
)



# Read table names from the file
$tableNames = Get-Content $tableNamesFile

# Function to execute SQL commands
function Execute-SqlCommand {
    param (
        [string]$server,
        [string]$database,
        [string]$sql
    )

    $connectionString = "Server=$server;Database=$database;Integrated Security=True;"
    $sqlConnection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
    $sqlCommand = $sqlConnection.CreateCommand()
    $sqlCommand.CommandText = $sql

    try {
        $sqlConnection.Open()
        $sqlCommand.ExecuteNonQuery()
    }
    catch {
        Log-Error "Error executing SQL command: $_"
    }
    finally {
        $sqlConnection.Close()
    }
}

# Function to check and create schema if it does not exist
function Check-CreateSchema {
    param (
        [string]$server,
        [string]$database,
        [string]$schema
    )

    $checkSchemaSql = @"
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = '$schema')
BEGIN
    EXEC('CREATE SCHEMA [$schema]')
END
"@
    Execute-SqlCommand -server $server -database $database -sql $checkSchemaSql
}

# Function to retrieve the CREATE TABLE script along with constraints and identity columns
function Get-CreateTableScript {
    param (
        [string]$server,
        [string]$database,
        [string]$schema,
        [string]$table
    )

    $connectionString = "Server=$server;Database=$database;Integrated Security=True;"
    $createTableSql = @"
DECLARE @sql NVARCHAR(MAX) = 'CREATE TABLE [' + '$schema' + '].[' + '$table' + '] ('

-- Get columns with identity properties
SELECT @sql += '[' + COLUMN_NAME + '] ' + DATA_TYPE +
       CASE 
           WHEN CHARACTER_MAXIMUM_LENGTH IS NOT NULL THEN 
               '(' + 
               CASE 
                   WHEN DATA_TYPE IN ('char', 'varchar', 'nchar', 'nvarchar') THEN 
                       CASE 
                           WHEN CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' 
                           ELSE CAST(CHARACTER_MAXIMUM_LENGTH AS NVARCHAR(10)) 
                       END 
                   ELSE CAST(NUMERIC_PRECISION AS NVARCHAR(10)) + 
                        CASE WHEN NUMERIC_SCALE > 0 THEN ',' + CAST(NUMERIC_SCALE AS NVARCHAR(10)) ELSE '' END 
               END + 
               ')' 
           ELSE ''
       END + 
       CASE WHEN c.is_nullable = 'NO' THEN ' NOT NULL' ELSE ' NULL' END + 
       CASE 
           WHEN ic.is_identity = 1 THEN 
               ' IDENTITY(' + CAST(ic.seed_value AS NVARCHAR(10)) + ',' + CAST(ic.increment_value AS NVARCHAR(10)) + ')' 
           ELSE '' 
       END + 
       ',' 
FROM INFORMATION_SCHEMA.COLUMNS c
LEFT JOIN sys.columns col ON col.name = c.COLUMN_NAME AND col.object_id = OBJECT_ID('$schema.$table')
LEFT JOIN sys.tables t ON t.object_id = OBJECT_ID('$schema.$table')
LEFT JOIN sys.identity_columns ic ON ic.object_id = t.object_id AND ic.name = col.name
WHERE c.TABLE_NAME = '$table' AND c.TABLE_SCHEMA = '$schema'

-- Remove the last comma
SET @sql = LEFT(@sql, LEN(@sql) - 1)

-- Add primary keys
SELECT @sql += ', CONSTRAINT [' + tc.CONSTRAINT_NAME + '] PRIMARY KEY (' + STRING_AGG('[' + kcu.COLUMN_NAME + ']', ', ') + ')'
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
WHERE tc.TABLE_NAME = '$table' AND tc.TABLE_SCHEMA = '$schema' AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
GROUP BY tc.CONSTRAINT_NAME

-- Add unique constraints
SELECT @sql += ', CONSTRAINT [' + tc.CONSTRAINT_NAME + '] UNIQUE (' + STRING_AGG('[' + kcu.COLUMN_NAME + ']', ', ') + ')'
FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
WHERE tc.TABLE_NAME = '$table' AND tc.TABLE_SCHEMA = '$schema' AND tc.CONSTRAINT_TYPE = 'UNIQUE'
GROUP BY tc.CONSTRAINT_NAME

-- Complete the CREATE TABLE statement
SET @sql += ')'
SELECT @sql
"@

    $createTableCommand = New-Object System.Data.SqlClient.SqlCommand($createTableSql, (New-Object System.Data.SqlClient.SqlConnection($connectionString)))
    $createTableScript = ""

    try {
        $createTableCommand.Connection.Open()
        $createTableScript = $createTableCommand.ExecuteScalar()
    }
    catch {
        Log-Error "Error retrieving CREATE TABLE script for $schema.$table $_"
    }
    finally {
        $createTableCommand.Connection.Close()
    }

    return $createTableScript
}

# Function to log errors to a file
function Log-Error {
    param (
        [string]$errorMessage
    )
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    $logMessage = "$timestamp - $errorMessage"
    Add-Content -Path $errorLogFile -Value $logMessage
}

# Loop through each table name
foreach ($table in $tableNames) {
    # Extract schema and table name
    $schema, $tableName = $table -split '\.'

    # Check and create the schema in the destination server
    Check-CreateSchema -server $destinationServer -database $destinationDatabase -schema $schema

    # Drop the table in the destination server if it exists
    $dropTableSql = "IF OBJECT_ID(N'[$schema].[$tableName]', N'TABLE') IS NOT NULL DROP TABLE [$schema].[$tableName];"
    Execute-SqlCommand -server $destinationServer -database $destinationDatabase -sql $dropTableSql

    # Get the CREATE TABLE script from the source server
    $createTableScript = Get-CreateTableScript -server $sourceServer -database $sourceDatabase -schema $schema -table $tableName

    # Create the table in the destination server
    if ($createTableScript) {
        Execute-SqlCommand -server $destinationServer -database $destinationDatabase -sql $createTableScript
        Write-Host "Table $schema.$tableName created in destination server."
    } else {
        Log-Error "No CREATE TABLE script found for $schema.$tableName."
    }
}