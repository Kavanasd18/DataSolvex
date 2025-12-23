# Parameters
param(
    [string]$serverName,         # Replace with your SQL Server name
    [string]$databaseName,       # Replace with your database name
    [string]$Tosch ,   # Schema to transfer to
    [string]$Fromsch ,   # Schema to transfer from
    [string]$logFolderPath        # Folder path for logging errors
)

# Ensure the log folder exists
if (-not (Test-Path -Path $logFolderPath)) {
    New-Item -ItemType Directory -Path $logFolderPath
}

# SQL Script
$sqlQuery = @"
DECLARE @sql NVARCHAR(400)
DECLARE @table_name VARCHAR(100)
DECLARE @stat VARCHAR(100)
DECLARE @counter INT
DECLARE @sch VARCHAR(200)
DECLARE @schema VARCHAR(20)
SET @counter = 1 
SET @schema = '$Tosch'
DECLARE @max INT 

-- Create a table to store all values 
IF OBJECT_ID(N'tempdb.dbo.#Schema_sp', N'U') IS NOT NULL  
   DROP TABLE #Schema_sp
IF OBJECT_ID(N'dbo.sp', N'U') IS NOT NULL  
   DROP TABLE sp

SET NOCOUNT ON

SELECT 
    schema_name(obj.schema_id) AS schema_name,
    obj.name AS proc_name,
    obj.type_desc AS ObjectType,
    SUBSTRING(par.parameters, 0, LEN(par.parameters)) AS parameters,
    mod.definition
INTO sp
FROM sys.objects obj
JOIN sys.sql_modules mod
    ON mod.object_id = obj.object_id
CROSS APPLY (
    SELECT p.name + ' ' + TYPE_NAME(p.user_type_id) + ', ' 
    FROM sys.parameters p
    WHERE p.object_id = obj.object_id 
          AND p.parameter_id != 0 
    FOR XML PATH('')
) par (parameters)
WHERE obj.type IN ('P', 'V', 'TR', 'FN', 'IF', 'TF') 
      AND schema_name(obj.schema_id) IN ('$Fromsch')
ORDER BY schema_name, proc_name;

SELECT 
    ROW_NUMBER() OVER (ORDER BY proc_name) AS id, 
    schema_name, 
    proc_name 
INTO #Schema_sp 
FROM sp 

SET @max = (SELECT COUNT(*) FROM #Schema_sp)

WHILE (@counter <= @max)
BEGIN 
    SET @stat = (SELECT schema_name FROM #Schema_sp WHERE id = @counter) 
    SET @table_name = (SELECT proc_name FROM #Schema_sp WHERE id = @counter)
    SET @sql = 'ALTER SCHEMA ' + @schema + ' TRANSFER [' + @stat + '].[' + @table_name + ']'
    EXEC (@sql)
    SET @counter = @counter + 1 
END
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