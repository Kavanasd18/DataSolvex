# Parameters
param(
    [string]$serverName,    # Replace with your SQL Server name
    [string]$databaseName ,  # Replace with your database name
    [string]$logFolderPath
    )

# Ensure the log folder exists
if (-not (Test-Path -Path $logFolderPath)) {
    New-Item -ItemType Directory -Path $logFolderPath
}

# SQL Script
$sqlQuery = @"
--Storing the data 
SELECT 
    schema_name(fk_tab.schema_id) AS fktable_schema,
    fk_tab.name AS fktable_name,
    schema_name(pk_tab.schema_id) AS pktable_schema,
    pk_tab.name AS pktable_name,
    SUBSTRING(fk_columns, 1, LEN(fk_columns) - 1) AS fkcolumn_name,
    SUBSTRING(pk_columns, 1, LEN(pk_columns) - 1) AS pkcolumn_name,
    fk.name AS fk_name
INTO fk
FROM sys.foreign_keys fk
    INNER JOIN sys.tables fk_tab 
        ON fk_tab.object_id = fk.parent_object_id
    INNER JOIN sys.tables pk_tab 
        ON pk_tab.object_id = fk.referenced_object_id
    CROSS APPLY (
        SELECT col.[name] + ', '
        FROM sys.foreign_key_columns fk_c
        INNER JOIN sys.columns col
            ON fk_c.parent_object_id = col.object_id
            AND fk_c.parent_column_id = col.column_id
        WHERE fk_c.constraint_object_id = fk.object_id
        ORDER BY fk_c.constraint_column_id -- Ensure the correct order of columns
        FOR XML PATH('')
    ) D(fk_columns)
    CROSS APPLY (
        SELECT col.[name] + ', '
        FROM sys.foreign_key_columns fk_c
        INNER JOIN sys.columns col
            ON fk_c.referenced_object_id = col.object_id
            AND fk_c.referenced_column_id = col.column_id
        WHERE fk_c.constraint_object_id = fk.object_id
        ORDER BY fk_c.constraint_column_id -- Ensure the correct order of columns
        FOR XML PATH('')
    ) E(pk_columns)
ORDER BY 
    schema_name(fk_tab.schema_id) + '.' + fk_tab.name,
    schema_name(pk_tab.schema_id) + '.' + pk_tab.name;
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
