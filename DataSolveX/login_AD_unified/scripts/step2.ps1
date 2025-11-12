# Parameters
param(
    [string]$serverName ,    # Replace with your SQL Server name
    [string]$databaseName ,  # Replace with your database name
    [string]$schemaName = 'lmn' , # Replace with your schema name
    [string]$logFolderPath   # Folder path for logging errors
)

# Ensure the log folder exists
if (-not (Test-Path -Path $logFolderPath)) {
    New-Item -ItemType Directory -Path $logFolderPath
}

# SQL Script
$sqlQuery = @"
-- Step 1: Create the Error Logging Table
IF OBJECT_ID('dbo.ErrorLog_Dropping_SchemaObjects', 'U') IS NOT NULL
    DROP TABLE dbo.ErrorLog_Dropping_SchemaObjects;

CREATE TABLE dbo.ErrorLog_Dropping_SchemaObjects (
    LogID INT IDENTITY(1,1) PRIMARY KEY,
    ErrorMessage NVARCHAR(MAX),
    ObjectName NVARCHAR(128),
    ObjectType NVARCHAR(50),
    LogDate DATETIME DEFAULT GETDATE()
);

-- Step 2: Define the Schema Name
DECLARE @SchemaName NVARCHAR(128) = '$schemaName';
DECLARE @sql NVARCHAR(MAX) = '';

-- Step 3: Drop Views
SELECT @sql += '
BEGIN TRY 
    DROP VIEW [' + s.name + '].[' + v.name + ']; 
END TRY 
BEGIN CATCH 
    INSERT INTO dbo.ErrorLog_Dropping_SchemaObjects (ErrorMessage, ObjectName, ObjectType)
    VALUES (ERROR_MESSAGE(), ''' + v.name + ''', ''View''); 
END CATCH;' + CHAR(13)
FROM sys.views v
JOIN sys.schemas s ON v.schema_id = s.schema_id
WHERE s.name = @SchemaName;

-- Step 4: Drop Functions
SELECT @sql += '
BEGIN TRY 
    DROP FUNCTION [' + s.name + '].[' + o.name + ']; 
END TRY 
BEGIN CATCH 
    INSERT INTO dbo.ErrorLog_Dropping_SchemaObjects (ErrorMessage, ObjectName, ObjectType)
    VALUES (ERROR_MESSAGE(), ''' + o.name + ''', ''Function''); 
END CATCH;' + CHAR(13)
FROM sys.objects o
JOIN sys.schemas s ON o.schema_id = s.schema_id
WHERE o.type IN ('FN', 'IF', 'TF')
AND s.name = @SchemaName;

-- Step 5: Drop Procedures
SELECT @sql += '
BEGIN TRY 
    DROP PROCEDURE [' + s.name + '].[' + p.name + ']; 
END TRY 
BEGIN CATCH 
    INSERT INTO dbo.ErrorLog_Dropping_SchemaObjects (ErrorMessage, ObjectName, ObjectType)
    VALUES (ERROR_MESSAGE(), ''' + p.name + ''', ''Procedure''); 
END CATCH;' + CHAR(13)
FROM sys.procedures p
JOIN sys.schemas s ON p.schema_id = s.schema_id
WHERE s.name = @SchemaName;

-- Step 6: Drop Tables
SELECT @sql += '
BEGIN TRY 
    DROP TABLE [' + s.name + '].[' + t.name + ']; 
END TRY 
BEGIN CATCH 
    INSERT INTO dbo.ErrorLog_Dropping_SchemaObjects (ErrorMessage, ObjectName, ObjectType)
    VALUES (ERROR_MESSAGE(), ''' + t.name + ''', ''Table''); 
END CATCH;' + CHAR(13)
FROM sys.tables t
JOIN sys.schemas s ON t.schema_id = s.schema_id
WHERE s.name = @SchemaName;

-- Step 7: Drop Triggers
SELECT @sql += '
BEGIN TRY 
    DROP TRIGGER [' + o.name + '].[' + t.name + ']; 
END TRY 
BEGIN CATCH 
    INSERT INTO dbo.ErrorLog_Dropping_SchemaObjects (ErrorMessage, ObjectName, ObjectType)
    VALUES (ERROR_MESSAGE(), ''' + t.name + ''', ''Trigger''); 
END CATCH;' + CHAR(13)
FROM sys.triggers t
JOIN sys.objects o ON t.parent_id = o.schema_id
WHERE t.name = @SchemaName;

-- Step 8: Print and execute the generated SQL
EXEC sp_executesql @sql;
"@

# Connection String
$connectionString = "Server=$serverName;Database=$databaseName;Integrated Security=True;TrustServerCertificate=True;"

try {
    # Execute the SQL query
    Invoke-Sqlcmd -ConnectionString $connectionString -Query $sqlQuery

    # Output success message
    Write-Host "SQL script executed successfully."
} catch {
    # Handle errors
    $errorMessage = "Error at $(Get-Date): $($_.Exception.Message)"
    $logFilePath = Join-Path -Path $logFolderPath -ChildPath 'ErrorLog.txt'

    # Write the error to the log file
    Add-Content -Path $logFilePath -Value $errorMessage

    # Display an error message
    Write-Host "An error occurred. Details logged to $logFilePath"
}
