# Parameters
param(
    [string]$serverName ,    # Replace with your SQL Server name
    [string]$databaseName ,  # Replace with your database name
    [string]$logFolderPath 
    )

# Ensure the log folder exists
if (-not (Test-Path -Path $logFolderPath)) {
    New-Item -ItemType Directory -Path $logFolderPath
}

# SQL Script
$sqlQuery = @"
--dropping FK

declare @sql nvarchar(max)
declare @fks varchar(max)
declare @ftbl varchar(max)
declare @fkc varchar(max)
declare @pks varchar(max)
declare @ptbl varchar(max)
declare @pkc varchar(max)
declare @fkn varchar(max)
declare @counter int
declare @max int 
set @counter=1 
--Create a table to store all values 
IF OBJECT_ID(N'tempdb.dbo.#drop_fk', N'U') IS NOT NULL  
   DROP TABLE #drop_fk
set nocount on

select row_number() over(order by fktable_name) id, * into #drop_fk from
fk

------------------------------------------------------------------
--where table_type<>'VIEW'
set @max = (select count(*) from #drop_fk)
--print @max 

while (@counter <= @max)
begin 
set @fks =(select fktable_schema from #drop_fk where id=@counter) 
set @ftbl = (select fktable_name from #drop_fk where id=@counter)
set @fkc = (select fkcolumn_name from #drop_fk where id=@counter)
set @pks = (select pktable_schema from #drop_fk where id=@counter)
set @ptbl = (select pktable_name from #drop_fk where id=@counter)
set @pkc = (select pkcolumn_name from #drop_fk where id=@counter)
set @fkn = (select fk_name from #drop_fk where id=@counter)
--print @table_name
-- ALTER TABLE [Person].[Address] DROP CONSTRAINT [FK_Address_StateProvince_StateProvinceID];
set @sql= 'ALTER TABLE '+'['+@fks+'].'+'['+@ftbl+'] DROP CONSTRAINT ['+@fkn+'] '
--print @sql
exec (@sql)
set @counter = @counter+1 
end 

----------------------------------------------
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
