/* ===============================================================
   SSIS PASSWORD ROTATION + CONFIG TABLE UPDATE
   CLEAN VERSION — FULLY COMPATIBLE WITH pyodbc
   No GO, No RAISERROR, No Debug Output
=============================================================== */

---------------------------------------------------------------
-- Step 1: Backup table
---------------------------------------------------------------
IF OBJECT_ID(N'__BACKUP_TABLE__', N'U') IS NOT NULL
    DROP TABLE __BACKUP_TABLE__;

SET NOCOUNT ON;

SELECT *, CAST(NULL AS VARCHAR(50)) AS status
INTO __BACKUP_TABLE__
FROM __CONFIG_TABLE__;


---------------------------------------------------------------
-- Step 2: Update ssis_pwd rotation values
---------------------------------------------------------------
UPDATE pwd
SET 
    old_password = current_password,
    current_password = CASE WHEN new_password IS NOT NULL THEN new_password ELSE current_password END,
    updated_by = SYSTEM_USER,
    updated_on = GETDATE()
FROM ssis_pwd pwd;


---------------------------------------------------------------
-- Step 3: Parse configuration strings using CTE
---------------------------------------------------------------
;WITH ConfigExtract AS (
    SELECT
        cfg.ConfiguredValue,
        cfg.ConfiguredValue AS OriginalConfiguredValue,
        cfg.ConfiguredValue AS RawConfiguredValue,
        LOWER(REPLACE(cfg.ConfiguredValue, ' ', '')) AS NormalizedConfig
    FROM __BACKUP_TABLE__ cfg
),
KeyDetection AS (
    SELECT *,
        CASE 
            WHEN CHARINDEX('Data Source=', ConfiguredValue) > 0 THEN 'Data Source='
            WHEN CHARINDEX('Server=', ConfiguredValue) > 0 THEN 'Server='
            WHEN CHARINDEX('DSN=', ConfiguredValue) > 0 THEN 'DSN='
            WHEN CHARINDEX('SQLServerName=', ConfiguredValue) > 0 THEN 'SQLServerName='
        END AS ActualDataSourceKey,

        CASE 
            WHEN CHARINDEX('User ID=', ConfiguredValue) > 0 THEN 'User ID='
            WHEN CHARINDEX('user id=', ConfiguredValue) > 0 THEN 'user id='
            WHEN CHARINDEX('UID=', ConfiguredValue) > 0 THEN 'UID='
            WHEN CHARINDEX('Username=', ConfiguredValue) > 0 THEN 'Username='
        END AS ActualUserIDKey,

        CASE 
            WHEN CHARINDEX('Password=', ConfiguredValue) > 0 THEN 'Password='
            WHEN CHARINDEX('Password =', ConfiguredValue) > 0 THEN 'Password ='
            WHEN CHARINDEX('PWD=', ConfiguredValue) > 0 THEN 'PWD='
            WHEN CHARINDEX('PWD =', ConfiguredValue) > 0 THEN 'PWD ='
        END AS ActualPasswordKey
    FROM ConfigExtract
),
Parsed AS (
    SELECT
        k.*,
        SUBSTRING(
            k.ConfiguredValue,
            CHARINDEX(k.ActualDataSourceKey, k.ConfiguredValue) + LEN(k.ActualDataSourceKey),
            CHARINDEX(';', k.ConfiguredValue + ';', CHARINDEX(k.ActualDataSourceKey, k.ConfiguredValue))
              - (CHARINDEX(k.ActualDataSourceKey, k.ConfiguredValue) + LEN(k.ActualDataSourceKey))
        ) AS ExtractedDataSource,

        SUBSTRING(
            k.ConfiguredValue,
            CHARINDEX(k.ActualUserIDKey, k.ConfiguredValue) + LEN(k.ActualUserIDKey),
            CHARINDEX(';', k.ConfiguredValue + ';', CHARINDEX(k.ActualUserIDKey, k.ConfiguredValue))
              - (CHARINDEX(k.ActualUserIDKey, k.ConfiguredValue) + LEN(k.ActualUserIDKey))
        ) AS ExtractedUserID
    FROM KeyDetection k
    WHERE 
        k.ActualDataSourceKey IS NOT NULL AND
        k.ActualUserIDKey IS NOT NULL AND
        k.ActualPasswordKey IS NOT NULL
)


---------------------------------------------------------------
-- Step 4: Update ConfiguredValue with rotated password
---------------------------------------------------------------
UPDATE cfg
SET 
    cfg.ConfiguredValue = STUFF(
        p.ConfiguredValue,
        CHARINDEX(p.ActualPasswordKey, p.ConfiguredValue) + LEN(p.ActualPasswordKey),
        CHARINDEX(';', p.ConfiguredValue + ';', CHARINDEX(p.ActualPasswordKey, p.ConfiguredValue))
          - (CHARINDEX(p.ActualPasswordKey, p.ConfiguredValue) + LEN(p.ActualPasswordKey)),
        pwd.current_password
    ),
    cfg.status = 'Changed'
FROM __BACKUP_TABLE__ cfg
JOIN (
    SELECT 
        p.ConfiguredValue,
        p.ActualPasswordKey,
        p.ExtractedDataSource,
        p.ExtractedUserID
    FROM Parsed p
    JOIN ssis_pwd pwd
      ON LOWER(REPLACE(p.ExtractedDataSource,' ','')) = LOWER(REPLACE(pwd.DataSource,' ','')) 
     AND LOWER(REPLACE(p.ExtractedUserID,' ',''))     = LOWER(REPLACE(pwd.UserID,' ','')) 
    WHERE pwd.new_password IS NOT NULL
) p 
    ON cfg.ConfiguredValue = p.ConfiguredValue
JOIN ssis_pwd pwd
  ON LOWER(REPLACE(p.ExtractedDataSource,' ','')) = LOWER(REPLACE(pwd.DataSource,' ','')) 
 AND LOWER(REPLACE(p.ExtractedUserID,' ',''))     = LOWER(REPLACE(pwd.UserID,' ','')); 

