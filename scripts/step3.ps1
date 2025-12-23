# Parameters
param(
    [string]$serverName,       # Replace with your SQL Server name
    [string]$databaseName,     # Replace with your database name
    [string]$Fromsch , # Schema to transfer from
    [string]$Tosch ,  # Schema to transfer to
    [string]$logFolderPath     # Folder path for logging errors
)

# Ensure the log folder exists
if (-not (Test-Path -Path $logFolderPath)) {
    New-Item -ItemType Directory -Path $logFolderPath
}

# SQL Script with dynamic schema names
$sqlQuery = @"
-- To change the schema of the table (transfer of schema)
DECLARE @sql NVARCHAR(400)
DECLARE @table_name VARCHAR(100)
DECLARE @stat VARCHAR(100)
DECLARE @counter INT
DECLARE @schema VARCHAR(20)
SET @counter = 1
SET @schema = '$Tosch' -- Specify the new schema name

DECLARE @max INT
-- Create a table to store all values
IF OBJECT_ID(N'tempdb.dbo.#Schema_change', N'U') IS NOT NULL  
   DROP TABLE #Schema_change

-- Collect all tables with the schema '$Fromsch'
SELECT schema_name(t.schema_id) AS schema_name,
       t.name AS table_name,
       'ALTER SCHEMA $Tosch TRANSFER ' + schema_name(t.schema_id) + '.' + t.name AS altr_stmnt
INTO #Schema_change
FROM sys.tables t
WHERE schema_name(t.schema_id) IN ('$Fromsch')

SELECT row_number() OVER (ORDER BY table_name) AS id, schema_name, table_name, altr_stmnt 
INTO #Schema_change_temp 
FROM #Schema_change 
ORDER BY 1

SET @max = (SELECT COUNT(*) FROM #Schema_change_temp)

-- Loop to execute schema transfer for each table
WHILE (@counter <= @max)
BEGIN
    SET @stat = (SELECT schema_name FROM #Schema_change_temp WHERE id = @counter) 
    SET @table_name = (SELECT table_name FROM #Schema_change_temp WHERE id = @counter)
    SET @sql = 'ALTER SCHEMA ' + @schema + ' TRANSFER [' + @stat + '].[' + @table_name + ']'
    EXEC(@sql)
    SET @counter = @counter + 1 
END

-- Repeat for stored procedures or other objects as needed
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