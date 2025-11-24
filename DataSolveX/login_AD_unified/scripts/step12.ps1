param (
    [string]$ServerName ,
    [string]$DatabaseName 
)

# The SQL query you provided, placed as the parameterized query
$query = @"
DECLARE @sql NVARCHAR(MAX)
DECLARE @fks VARCHAR(MAX)
DECLARE @ftbl VARCHAR(MAX)
DECLARE @fkc VARCHAR(MAX)
DECLARE @pks VARCHAR(MAX)
DECLARE @ptbl VARCHAR(MAX)
DECLARE @pkc VARCHAR(MAX)
DECLARE @fkn VARCHAR(MAX)
DECLARE @counter INT
DECLARE @max INT

SET @counter = 1

-- Create a table to store all values 
IF OBJECT_ID(N'tempdb.dbo.#create_fk', N'U') IS NOT NULL  
   DROP TABLE #create_fk
SET NOCOUNT ON

SELECT ROW_NUMBER() OVER (ORDER BY fktable_name) AS id, * 
INTO #create_fk 
FROM fk

-- Get the maximum row count
SET @max = (SELECT COUNT(*) FROM #create_fk)

-- Iterate through the FK constraints
WHILE (@counter <= @max)
BEGIN 
    SET @fks = (SELECT fktable_schema FROM #create_fk WHERE id = @counter) 
    SET @ftbl = (SELECT fktable_name FROM #create_fk WHERE id = @counter)
    SET @fkc = (SELECT fkcolumn_name FROM #create_fk WHERE id = @counter)
    SET @pks = (SELECT pktable_schema FROM #create_fk WHERE id = @counter)
    SET @ptbl = (SELECT pktable_name FROM #create_fk WHERE id = @counter)
    SET @pkc = (SELECT pkcolumn_name FROM #create_fk WHERE id = @counter)
    SET @fkn = (SELECT fk_name FROM #create_fk WHERE id = @counter)
    
    SET @sql = 'ALTER TABLE [' + @fks + '].[' + @ftbl + '] WITH NOCHECK ADD CONSTRAINT [' + @fkn + '] FOREIGN KEY (' + @fkc + ') REFERENCES [' + @pks + '].[' + @ptbl + '](' + @pkc + ')'
    
    -- Error handling: Use TRY...CATCH to prevent script from stopping
    BEGIN TRY
        EXEC (@sql)
    END TRY
    BEGIN CATCH
        -- Capture the error and log it, but continue processing
        PRINT 'Error in adding FK: ' + ERROR_MESSAGE()
    END CATCH

    SET @counter = @counter + 1 
END
"@

# Ensure the SQL Server module is installed
if (-not (Get-Module -ListAvailable -Name SqlServer)) {
    Install-Module -Name SqlServer -Force -Scope CurrentUser
}

# Import the module
Import-Module SqlServer

# Create the connection string with SSL trust bypass
$connectionString = "Server=$ServerName;Database=$DatabaseName;Integrated Security=True;TrustServerCertificate=True;"

# Execute the SQL query using Invoke-Sqlcmd with the connection string
Invoke-Sqlcmd -ConnectionString $connectionString -Query $query