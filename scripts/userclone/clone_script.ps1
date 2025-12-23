param(
    [string]$ServerName,
    [string]$DBName,
    [string]$LoggingServer,
    [string]$LoggingDB,
    [string]$Mode,        # "Single" or "Multiple"
    [string]$UserPair,    # "old,new"
    [string]$UserFile     # path to .txt (for multiple)
)

$ErrorActionPreference = 'Stop'

function Emit-Progress {
    param(
        [int]$Percent,
        [string]$Stage
    )
    Write-Output ("__PROGRESS__:{0}:{1}" -f $Percent, $Stage)
}


Emit-Progress 5 "START"

$serverInstance = $ServerName
$databaseName   = $DBName

# Primary = cloning DB
$sourceConnectionString = "Server=$serverInstance;Database=$databaseName;Integrated Security=True;Connection Timeout=600;"

# Logging = central logging DB
$destConnectionString   = "Server=$LoggingServer;Database=$LoggingDB;Integrated Security=True;Connection Timeout=600;"

# Logging DB connection for CloneValidation
$logConnection = New-Object System.Data.SqlClient.SqlConnection
$logConnection.ConnectionString = $destConnectionString

# Normalize mode
$mode = $Mode.ToLower().Trim()

$singleLoginPair = $UserPair
$loginFilePath   = $UserFile

# track output file path for later messages
$outputFileFullPath = ""

# ----------------- BUILD LOGIN PAIRS -----------------
$loginPairs = @()

if ($mode -eq "single") {
    $parts = $singleLoginPair.Split(',')
    if ($parts.Count -ne 2 -or $parts[0].Trim() -eq "" -or $parts[1].Trim() -eq "") {
        Write-Host "Error: Invalid single login pair. Provide as 'OldLogin,NewLogin'." -ForegroundColor Red
        exit 1
    }
    $loginPairs += [PSCustomObject]@{
        OldLogin = $parts[0].Trim()
        NewLogin = $parts[1].Trim()
    }
}
elseif ($mode -eq "multiple") {
    if (-not (Test-Path $loginFilePath)) {
        Write-Host "Error: Login file not found at '$loginFilePath'!" -ForegroundColor Red
        exit 1
    }

    Get-Content $loginFilePath | ForEach-Object {
        $line = $_.Trim()
        if ($line -match '^[^,]+,[^,]+$') {
            $parts = $line.Split(',')
            $loginPairs += [PSCustomObject]@{
                OldLogin = $parts[0].Trim()
                NewLogin = $parts[1].Trim()
            }
        }
        elseif ($line -ne "") {
            Write-Warning "Skipping malformed line '$line'. Expected format: OldLogin,NewLogin"
        }
    }

    if ($loginPairs.Count -eq 0) {
        Write-Host "Error: No valid login pairs found in file." -ForegroundColor Red
        exit 1
    }
}
else {
    Write-Host "Error: Invalid mode selected. Choose 'single' or 'multiple'." -ForegroundColor Red
    exit 1
}

# ----------------- PRE-VALIDATION LOGIC (SINGLE vs MULTIPLE) -----------------
$connectionString = "Server=$serverInstance;Database=$databaseName;Integrated Security=True;TrustServerCertificate=True;Connection Timeout=6000;"
$preConn = New-Object System.Data.SqlClient.SqlConnection
$preConn.ConnectionString = $connectionString

$validPairs   = @()
$invalidPairs = @()

try {
    $preConn.Open()

    foreach ($p in $loginPairs) {
        $escapedOld = $p.OldLogin.Replace("'", "''")
        $cmd = $preConn.CreateCommand()
        $cmd.CommandText = "SELECT name FROM sys.server_principals WHERE name = N'$escapedOld';"
        $exists = $cmd.ExecuteScalar()

        if ($exists) {
            $validPairs += $p
        }
        else {
            $invalidPairs += $p
        }
    }

    if ($mode -eq "single") {
        if ($invalidPairs.Count -gt 0) {
            $bad = $invalidPairs[0].OldLogin
            Write-Host "ERROR: Old login '$bad' does not exist on server '$serverInstance'." -ForegroundColor Red
            Write-Host "Terminating without cloning." -ForegroundColor Red
            exit 1
        }

        Write-Host "Pre-validation OK: Old login exists. Proceeding with cloning."
        $loginPairs = $validPairs
    }
    else {
        # multiple mode
        if ($invalidPairs.Count -gt 0) {
            # Build invalid content in-memory (old,new lines)
            $invalidLines = $invalidPairs | ForEach-Object { "$($_.OldLogin),$($_.NewLogin)" }
            $invalidContent = ($invalidLines -join "`r`n") + "`r`n"

            # Determine folder of the user-provided file
            $originalFolder = Split-Path $loginFilePath -Parent
            if ([string]::IsNullOrWhiteSpace($originalFolder)) {
                # fallback to current directory if parent not resolvable
                $originalFolder = Get-Location
            }

            # generate timestamped filename
            $timestamp = (Get-Date).ToString('yyyy-MM-dd_HH-mm-ss')
            $outputFileName = "invalid_clone_$timestamp.txt"
            $outputFileFullPath = Join-Path $originalFolder $outputFileName

            # Write invalid file to disk ONLY (no browser download)
            try {
                Set-Content -Path $outputFileFullPath -Value $invalidContent -Encoding UTF8
            }
            catch {
                Write-Host "ERROR: Failed to write invalid file to path: $outputFileFullPath. $_" -ForegroundColor Red
                # still continue, but notify user
            }

            # Informational note for console (kept minimal)
            Write-Host "NOTE: Some logins do not exist. Invalid entries written to: $outputFileFullPath" -ForegroundColor Yellow

            # proceed only with valid pairs
            $loginPairs = $validPairs
        }

        if ($loginPairs.Count -eq 0) {
            Write-Host "ERROR: No valid logins to clone. All old logins are invalid." -ForegroundColor Red
            exit 1
        }

        Write-Host "Pre-validation complete: $($validPairs.Count) valid users, $($invalidPairs.Count) invalid."
    }
}
catch {
    Write-Host "Pre-validation error: $_" -ForegroundColor Red
    exit 1
}
finally {
    if ($preConn.State -eq 'Open') { $preConn.Close() }
}

Emit-Progress 10 "PRECHECK_DONE"

# ---  MAIN CONNECTION, LOOP, SQL CLONING BLOCK ---
$connection = New-Object System.Data.SqlClient.SqlConnection
$connection.ConnectionString = $connectionString

try {
    $connection.Open()
    Write-Host "Connected to SQL Server successfully."

    # Open logging connection and ensure CloneValidation table
    $logConnection.Open()
    $logEnsureCmd = $logConnection.CreateCommand()
    $logEnsureCmd.CommandText = @"
    SET NOCOUNT ON;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'CloneValidation')
BEGIN
    CREATE TABLE CloneValidation (
        OldUser NVARCHAR(100),
        NewUser NVARCHAR(100),
        ValidationStatus NVARCHAR(20),
        [Timestamp] DATETIME DEFAULT GETDATE(),
        Remarks NVARCHAR(255)
    );
END
"@
    $logEnsureCmd.ExecuteNonQuery()

    foreach ($pair in $loginPairs) {

        $oldLogin = $pair.OldLogin
        $newLogin = $pair.NewLogin

        if ($oldLogin -eq "" -or $newLogin -eq "") {
            Write-Warning "Skipping empty old or new login name in pair: OldLogin='$oldLogin', NewLogin='$newLogin'"
            continue
        }

        $escapedOldLogin = $oldLogin -replace "'", "''"
        $escapedNewLogin = $newLogin -replace "'", "''"
        $whoCreated      = $env:USERNAME

        # Extra server-level check for safety (should normally pass because of pre-check)
        $preCheckCmd = $connection.CreateCommand()
        $preCheckCmd.CommandText = @"
        SET NOCOUNT ON;

IF NOT EXISTS (
    SELECT 1
    FROM sys.server_principals
    WHERE name = N'$escapedOldLogin'
)
BEGIN
    THROW 50000, 'OLD LOGIN NOT FOUND ON SERVER', 1;
END
"@
        try {
            $preCheckCmd.ExecuteNonQuery()
        }
        catch {
            Write-Host "ERROR: Old login '$oldLogin' does not exist on server. Aborting this pair." -ForegroundColor Red
            throw
        }

        # ---------------- PRIMARY SQL BLOCK (UNTOUCHED CORE) ----------------
        $sqlQueryPrimary = @"
        SET NOCOUNT ON;

DECLARE @OldLogin NVARCHAR(100) = N'$escapedOldLogin';
DECLARE @NewLogin NVARCHAR(100) = N'$escapedNewLogin';
DECLARE @NewPassword NVARCHAR(100);
DECLARE @WhoCreated NVARCHAR(100) = N'$whoCreated';
DECLARE @DBName NVARCHAR(100);
DECLARE @UserName NVARCHAR(100);
DECLARE @SQL NVARCHAR(MAX);

SELECT @NewPassword =
    CHAR(65 + ABS(CHECKSUM(NEWID())) % 26) +
    CHAR(97 + ABS(CHECKSUM(NEWID())) % 26) +
    CHAR(97 + ABS(CHECKSUM(NEWID())) % 26) +
    CHAR(65 + ABS(CHECKSUM(NEWID())) % 26) +
    CAST(ABS(CHECKSUM(NEWID())) % 9000 + 1000 AS VARCHAR) +
    (SELECT TOP 1 val FROM (VALUES ('+'), ('-'), ('!'), ('@'), ('#'), ('$'), ('%'), ('^'), ('&'), ('*')) AS Special(val) ORDER BY NEWID()) +
    CHAR(97 + ABS(CHECKSUM(NEWID())) % 26) +
    CHAR(97 + ABS(CHECKSUM(NEWID())) % 26) +
    CHAR(97 + ABS(CHECKSUM(NEWID())) % 26) +
    CHAR(97 + ABS(CHECKSUM(NEWID())) % 26) +
    CHAR(97 + ABS(CHECKSUM(NEWID())) % 26);

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ClonedUserDetails')
BEGIN
    CREATE TABLE ClonedUserDetails (
        OldLogin NVARCHAR(100),
        NewLogin NVARCHAR(100),
        GeneratedPassword NVARCHAR(100),
        WhoCreated NVARCHAR(100),
        CreatedAt DATETIME DEFAULT GETDATE()
    );
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ClonedUserPermissions')
BEGIN
    CREATE TABLE ClonedUserPermissions (
        DatabaseName NVARCHAR(100),
        ClonedUser NVARCHAR(100),
        PermissionState NVARCHAR(20),
        PermissionName NVARCHAR(100),
        ObjectName NVARCHAR(255),
        ObjectType NVARCHAR(50),
        GrantedBy NVARCHAR(100),
        GrantedAt DATETIME DEFAULT GETDATE()
    );
END

CREATE TABLE #DatabasesWithOldLogin (DatabaseName NVARCHAR(100));

INSERT INTO #DatabasesWithOldLogin
EXEC sp_MSforeachdb '
    IF EXISTS (SELECT * FROM ?.sys.database_principals WHERE sid = SUSER_SID(''$escapedOldLogin''))
    BEGIN
        SELECT ''?'' AS DatabaseName;
    END
';

IF NOT EXISTS (SELECT * FROM sys.sql_logins WHERE name = @NewLogin)
BEGIN
    SET @SQL = 'CREATE LOGIN [' + @NewLogin + '] WITH PASSWORD = ''' + @NewPassword + ''', CHECK_POLICY = OFF;';
    EXEC sp_executesql @SQL;

    INSERT INTO ClonedUserDetails (OldLogin, NewLogin, GeneratedPassword, WhoCreated)
    VALUES (@OldLogin, @NewLogin, @NewPassword, @WhoCreated);
END

DECLARE db_cursor CURSOR FOR SELECT DatabaseName FROM #DatabasesWithOldLogin;
OPEN db_cursor;
FETCH NEXT FROM db_cursor INTO @DBName;

WHILE @@FETCH_STATUS = 0
BEGIN

    DECLARE @UserSQL NVARCHAR(MAX) = '
        USE [' + @DBName + '];
        SELECT name FROM sys.database_principals WHERE sid = SUSER_SID(''' + @OldLogin + ''');
    ';

    CREATE TABLE #Users (UserName NVARCHAR(100));
    INSERT INTO #Users EXEC sp_executesql @UserSQL;

    DECLARE user_cursor CURSOR FOR SELECT UserName FROM #Users;
    OPEN user_cursor;
    FETCH NEXT FROM user_cursor INTO @UserName;

    WHILE @@FETCH_STATUS = 0
    BEGIN

        IF NOT EXISTS (
            SELECT * FROM sys.database_principals WHERE name = @NewLogin AND sid = SUSER_SID(@NewLogin)
        )
        BEGIN
            SET @SQL = 'USE [' + @DBName + ']; CREATE USER [' + @NewLogin + '] FOR LOGIN [' + @NewLogin + '];';
            EXEC sp_executesql @SQL;
        END

        DECLARE @RoleSQL NVARCHAR(MAX);
        SET @RoleSQL = '
            USE [' + @DBName + '];
            DECLARE @RoleName NVARCHAR(128);
            DECLARE role_cursor CURSOR FOR
            SELECT rp.name
            FROM sys.database_role_members rm
            JOIN sys.database_principals rp ON rm.role_principal_id = rp.principal_id
            JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
            WHERE dp.name = ''' + @UserName + ''';

            OPEN role_cursor;
            FETCH NEXT FROM role_cursor INTO @RoleName;

            WHILE @@FETCH_STATUS = 0
            BEGIN
                EXEC sp_addrolemember @RoleName, ''' + @NewLogin + ''';
                FETCH NEXT FROM role_cursor INTO @RoleName;
            END

            CLOSE role_cursor;
            DEALLOCATE role_cursor;
        ';
        EXEC sp_executesql @RoleSQL;

        DECLARE @PermissionSQL NVARCHAR(MAX) = '
            USE [' + @DBName + '];
            SELECT dp.state_desc,
                    dp.permission_name,
                    COALESCE(sc.name + ''.'' + so.name, ''DATABASE'') AS ObjectName,
                    COALESCE(so.type_desc, ''DATABASE'') AS ObjectType,
                    SUSER_NAME(dp.grantor_principal_id) AS GrantedBy
            FROM sys.database_permissions dp
            JOIN sys.database_principals du ON dp.grantee_principal_id = du.principal_id
            LEFT JOIN sys.objects so ON dp.major_id = so.object_id
            LEFT JOIN sys.schemas sc ON so.schema_id = sc.schema_id
            WHERE du.name = ''' + @UserName + ''';
        ';

        CREATE TABLE #Permissions (
            PermissionState NVARCHAR(20),
            PermissionName NVARCHAR(100),
            ObjectName NVARCHAR(255),
            ObjectType NVARCHAR(50),
            GrantedBy NVARCHAR(100)
        );

        INSERT INTO #Permissions EXEC sp_executesql @PermissionSQL;

        DECLARE @PermissionState NVARCHAR(20), @PermissionName NVARCHAR(100), @ObjectName NVARCHAR(255), @ObjectType NVARCHAR(50), @GrantedBy NVARCHAR(100);

        DECLARE PermissionCursor CURSOR FOR
            SELECT PermissionState, PermissionName, ObjectName, ObjectType, GrantedBy FROM #Permissions;

        OPEN PermissionCursor;
        FETCH NEXT FROM PermissionCursor INTO @PermissionState, @PermissionName, @ObjectName, @ObjectType, @GrantedBy;

        WHILE @@FETCH_STATUS = 0
        BEGIN

            IF @PermissionState = 'GRANT'
                SET @SQL = 'USE [' + @DBName + ']; GRANT ' + @PermissionName + ' ON ' +
                    CASE WHEN @ObjectName = 'DATABASE' THEN 'DATABASE::[' + @DBName + ']'
                         ELSE 'OBJECT::[' + REPLACE(@ObjectName, '.', '].[') + ']' END +
                    ' TO [' + @NewLogin + '];';

            ELSE IF @PermissionState = 'DENY'
                SET @SQL = 'USE [' + @DBName + ']; DENY ' + @PermissionName + ' ON ' +
                    CASE WHEN @ObjectName = 'DATABASE' THEN 'DATABASE::[' + @DBName + ']'
                         ELSE 'OBJECT::[' + REPLACE(@ObjectName, '.', '].[') + ']' END +
                    ' TO [' + @NewLogin + '];';

            ELSE IF @PermissionState = 'REVOKE'
                SET @SQL = 'USE [' + @DBName + ']; REVOKE ' + @PermissionName + ' ON ' +
                    CASE WHEN @ObjectName = 'DATABASE' THEN 'DATABASE::[' + @DBName + ']'
                         ELSE 'OBJECT::[' + REPLACE(@ObjectName, '.', '].[') + ']' END +
                    ' FROM [' + @NewLogin + '];';

            EXEC sp_executesql @SQL;

            INSERT INTO ClonedUserPermissions (DatabaseName, ClonedUser, PermissionState, PermissionName, ObjectName, ObjectType, GrantedBy)
            VALUES (@DBName, @NewLogin, @PermissionState, @PermissionName, @ObjectName, @ObjectType, @GrantedBy);

            FETCH NEXT FROM PermissionCursor INTO @PermissionState, @PermissionName, @ObjectName, @ObjectType, @GrantedBy;
        END

        CLOSE PermissionCursor;
        DEALLOCATE PermissionCursor;
        DROP TABLE #Permissions;

        FETCH NEXT FROM user_cursor INTO @UserName;
    END

    CLOSE user_cursor;
    DEALLOCATE user_cursor;
    DROP TABLE #Users;

    FETCH NEXT FROM db_cursor INTO @DBName;
END

CLOSE db_cursor;
DEALLOCATE db_cursor;
DROP TABLE #DatabasesWithOldLogin;

PRINT 'New login created: ' + @NewLogin;
PRINT 'Generated password: ' + @NewPassword;
"@
        Write-Host "Executing SQL for: $oldLogin -> $newLogin"
        $command = $connection.CreateCommand()
        $command.CommandText = $sqlQueryPrimary
        $command.ExecuteNonQuery()
        Write-Host "Cloned login: $oldLogin -> $newLogin"
        Emit-Progress 30 "CLONE_DONE"

    } # end foreach pair

    Emit-Progress 35 "CLONE_LOOP_DONE"

# --- VALIDATION BLOCK (LOGIN + ROLE + PERMISSION) ---
    foreach ($pair in $loginPairs) {
        $oldLogin = $pair.OldLogin
        $newLogin = $pair.NewLogin
        $escapedOldLogin = $oldLogin -replace "'", "''"
        $escapedNewLogin = $newLogin -replace "'", "''"

        Write-Host "Validating clone: $oldLogin vs $newLogin"

        # 1. LOGIN-LEVEL CHECK
        $loginCheckQuery = @"
        SET NOCOUNT ON;

SELECT CASE
         WHEN o.type = n.type
          AND o.is_disabled = n.is_disabled
         THEN 'OK'
         ELSE 'LOGIN_MISMATCH'
       END
FROM sys.server_principals o
JOIN sys.server_principals n ON n.name = N'$escapedNewLogin'
WHERE o.name = N'$escapedOldLogin';
"@
        $cmd = $connection.CreateCommand()
        $cmd.CommandText = $loginCheckQuery
        $loginResult = $cmd.ExecuteScalar()

        if ($loginResult -ne "OK") {

            if ($logConnection.State -eq 'Open') {
                $logCmd = $logConnection.CreateCommand()
                $logCmd.CommandText = @"
                SET NOCOUNT ON;

INSERT INTO CloneValidation (OldUser, NewUser, ValidationStatus, Remarks)
VALUES (N'$escapedOldLogin', N'$escapedNewLogin', N'FAILED', N'LOGIN_MISMATCH');
"@
                $logCmd.ExecuteNonQuery()
            }

            Write-Host "Mismatch detected (LOGIN_MISMATCH). Dropping cloned user $newLogin"
            $dropQuery = "IF EXISTS (SELECT * FROM sys.server_principals WHERE name=N'$escapedNewLogin') DROP LOGIN [$escapedNewLogin];"
            $cmd.CommandText = $dropQuery
            $cmd.ExecuteNonQuery()
            Emit-Progress 60 "VALIDATION_DONE"

            continue
        }

        # 2. ROLE + PERMISSION CHECK PER DATABASE
        $dbListCmd = "SELECT name FROM sys.databases WHERE state_desc='ONLINE';"
        $cmd.CommandText = $dbListCmd
        $reader = $cmd.ExecuteReader()
        $dbList = @()
        while ($reader.Read()) { $dbList += $reader.GetString(0) }
        $reader.Close()

        $globalMismatch = $false

        foreach ($db in $dbList) {

            $dbEsc = $db.Replace("'", "''")

            # ROLE MATCH CHECK
            $roleQuery = @"
            SET NOCOUNT ON;

DECLARE @sql NVARCHAR(MAX);
DECLARE @result NVARCHAR(20);

SET @sql = N'
SELECT CASE WHEN NOT EXISTS (
    SELECT role_principal_id, member_principal_id
    FROM sys.database_role_members rm
    JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
    WHERE dp.name = N''$escapedOldLogin''
    EXCEPT
    SELECT role_principal_id, member_principal_id
    FROM sys.database_role_members rm
    JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
    WHERE dp.name = N''$escapedNewLogin''
)
AND NOT EXISTS (
    SELECT role_principal_id, member_principal_id
    FROM sys.database_role_members rm
    JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
    WHERE dp.name = N''$escapedNewLogin''
    EXCEPT
    SELECT role_principal_id, member_principal_id
    FROM sys.database_role_members rm
    JOIN sys.database_principals dp ON rm.member_principal_id = dp.principal_id
    WHERE dp.name = N''$escapedOldLogin''
)
THEN ''OK'' ELSE ''ROLE_MISMATCH'' END AS Result;
';

EXEC (N'USE [$dbEsc]; ' + @sql);
"@

            $cmd.CommandText = $roleQuery
            $roleResult = $cmd.ExecuteScalar()

            if ($roleResult -ne "OK") {
                $globalMismatch = $true
                break
            }

            # PERMISSION MATCH CHECK
            $permQuery = @"
            SET NOCOUNT ON;

DECLARE @sql NVARCHAR(MAX);

SET @sql = N'
SELECT CASE WHEN NOT EXISTS (
    SELECT state_desc, permission_name, major_id, class
    FROM sys.database_permissions dp
    JOIN sys.database_principals du ON dp.grantee_principal_id = du.principal_id
    WHERE du.name = N''$escapedOldLogin''
    EXCEPT
    SELECT state_desc, permission_name, major_id, class
    FROM sys.database_permissions dp
    JOIN sys.database_principals du ON dp.grantee_principal_id = du.principal_id
    WHERE du.name = N''$escapedNewLogin''
)
AND NOT EXISTS (
    SELECT state_desc, permission_name, major_id, class
    FROM sys.database_permissions dp
    JOIN sys.database_principals du ON dp.grantee_principal_id = du.principal_id
    WHERE du.name = N''$escapedNewLogin''
    EXCEPT
    SELECT state_desc, permission_name, major_id, class
    FROM sys.database_permissions dp
    JOIN sys.database_principals du ON dp.grantee_principal_id = du.principal_id
    WHERE du.name = N''$escapedOldLogin''
)
THEN ''OK'' ELSE ''PERM_MISMATCH'' END AS Result;
';

EXEC (N'USE [$dbEsc]; ' + @sql);
"@

            $cmd.CommandText = $permQuery
            $permResult = $cmd.ExecuteScalar()

            if ($permResult -ne "OK") {
                $globalMismatch = $true
                break
            }
        }

        if ($globalMismatch) {

            if ($logConnection.State -eq 'Open') {
                $logCmd = $logConnection.CreateCommand()
                $logCmd.CommandText = @"
                SET NOCOUNT ON;

INSERT INTO CloneValidation (OldUser, NewUser, ValidationStatus, Remarks)
VALUES (N'$escapedOldLogin', N'$escapedNewLogin', N'FAILED', N'ROLE_OR_PERMISSION_MISMATCH');
"@
                $logCmd.ExecuteNonQuery()
            }

            Write-Host "Mismatch detected. Dropping cloned user $newLogin"
            $dropQuery = "IF EXISTS (SELECT * FROM sys.server_principals WHERE name=N'$escapedNewLogin') DROP LOGIN [$escapedNewLogin];"
            $cmd.CommandText = $dropQuery
            $cmd.ExecuteNonQuery()
            Emit-Progress 60 "VALIDATION_DONE"

            continue
        }

        Write-Host "User match OK: $oldLogin and $newLogin are identical"

        if ($logConnection.State -eq 'Open') {
            $logCmd = $logConnection.CreateCommand()
            $logCmd.CommandText = @"
            SET NOCOUNT ON;

INSERT INTO CloneValidation (OldUser, NewUser, ValidationStatus, Remarks)
VALUES (N'$escapedOldLogin', N'$escapedNewLogin', N'SUCCESS', N'IDENTICAL');
"@
            $logCmd.ExecuteNonQuery()
        }
        Emit-Progress 60 "VALIDATION_DONE"

    } # end validation foreach

# --- AG / SECONDARY REPLICA CREATION & VALIDATION ---
    foreach ($pair in $loginPairs) {
        $oldLogin = $pair.OldLogin
        $newLogin = $pair.NewLogin
        $escapedNewLogin = $newLogin -replace "'", "''"

        Write-Host "Retrieving SID and password hash for '$newLogin'..."
        $getSidHashQuery = "SELECT sid, password_hash FROM sys.sql_logins WHERE name = '$escapedNewLogin'"
        $command = $connection.CreateCommand()
        $command.CommandText = $getSidHashQuery
        $reader = $command.ExecuteReader()
        $sid     = $null
        $pwdhash = $null
        if ($reader.Read()) {
            $sid     = $reader.GetValue(0)
            $pwdhash = $reader.GetValue(1)
        }
        $reader.Close()

        if ($null -eq $sid) {
            Write-Warning "Could not retrieve SID and password hash for '$newLogin'. Skipping secondary replica creation."
            continue
        }

        Write-Host "Checking for secondary replicas..."
        $secondaryReplicasQuery = @"
        SET NOCOUNT ON;

SELECT r.replica_server_name
FROM sys.availability_replicas r
JOIN sys.dm_hadr_availability_replica_states rs
    ON r.replica_id = rs.replica_id
WHERE rs.role_desc = 'SECONDARY';
"@

        $secondaryReplicas = @()
        $command.CommandText = $secondaryReplicasQuery
        $reader = $command.ExecuteReader()
        while ($reader.Read()) { $secondaryReplicas += $reader.GetValue(0) }
        $reader.Close()

        if ($secondaryReplicas.Count -gt 0) {

            # Hex values from primary
            $sidHex     = [System.BitConverter]::ToString($sid).Replace("-", "")
            $pwdHashHex = [System.BitConverter]::ToString($pwdhash).Replace("-", "")

            # Create logins on all secondary replicas
            foreach ($secondaryServer in $secondaryReplicas) {

                Write-Host "Connecting to secondary server: $secondaryServer to create login '$newLogin'..."
                $secondaryConnectionString = "Server=$secondaryServer;Integrated Security=True;TrustServerCertificate=True;Connection Timeout=6000;"
                $secondaryConnection = New-Object System.Data.SqlClient.SqlConnection
                $secondaryConnection.ConnectionString = $secondaryConnectionString

                try {
                    $secondaryConnection.Open()

                    $secondaryLoginSQL = "
IF NOT EXISTS (SELECT name FROM sys.sql_logins WHERE name = '$escapedNewLogin')
BEGIN
    CREATE LOGIN [$escapedNewLogin] WITH PASSWORD = 0x$pwdHashHex HASHED, SID = 0x$sidHex, CHECK_POLICY = OFF;
    PRINT 'Successfully created login [$escapedNewLogin] on secondary server: [$secondaryServer]';
END
"
                    $secondaryCommand = $secondaryConnection.CreateCommand()
                    $secondaryCommand.CommandText = $secondaryLoginSQL
                    $secondaryCommand.ExecuteNonQuery()
                }
                catch {
                    Write-Host "Error creating login on secondary server $secondaryServer $_" -ForegroundColor Red
                }
                finally {
                    $secondaryConnection.Close()
                }
            }

            # AG VALIDATION on each secondary
            foreach ($secondaryServer in $secondaryReplicas) {

                Write-Host "Validating login on secondary replica: $secondaryServer"

                $validateConn = New-Object System.Data.SqlClient.SqlConnection
                $validateConn.ConnectionString = "Server=$secondaryServer;Integrated Security=True;TrustServerCertificate=True;Connection Timeout=6000;"

                try {
                    $validateConn.Open()

                    $validateCmd = $validateConn.CreateCommand()
                    $validateCmd.CommandText = @"
                    SET NOCOUNT ON;

SELECT
    CASE
        WHEN sid = 0x$sidHex
         AND password_hash = 0x$pwdHashHex
        THEN 'OK'
        ELSE 'LOGIN_MISMATCH'
    END
FROM sys.sql_logins
WHERE name = N'$escapedNewLogin';
"@

                    $result = $validateCmd.ExecuteScalar()

                    if ($result -ne 'OK') {
                        Write-Host "AG VALIDATION FAILED on $secondaryServer for login $newLogin" -ForegroundColor Red
                        exit 1
                    }

                    Write-Host "AG validation OK on $secondaryServer"

                }
                finally {
                    $validateConn.Close()
                }
            }
        }
        else {
            Write-Host "No secondary replicas found for AG. Skipping login creation on secondaries."
        }
    }

    Emit-Progress 80 "AG_DONE"

# ---DATA MIGRATION, TRUNCATE, CLEANUP ---
    # ------------------- DATA MIGRATION BLOCK -------------------

    $sourceConn = New-Object System.Data.SqlClient.SqlConnection($sourceConnectionString)
    $destConn   = New-Object System.Data.SqlClient.SqlConnection($destConnectionString)

    $sourceConn.Open()
    $destConn.Open()

    try {
     
        $ensureDestSql = @"
        SET NOCOUNT ON;

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ClonedUserDetails')
BEGIN
    CREATE TABLE ClonedUserDetails (
        OldLogin NVARCHAR(100),
        NewLogin NVARCHAR(100),
        GeneratedPassword NVARCHAR(100),
        WhoCreated NVARCHAR(100),
        CreatedAt DATETIME DEFAULT GETDATE()
    );
END

IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'ClonedUserPermissions')
BEGIN
    CREATE TABLE ClonedUserPermissions (
        DatabaseName NVARCHAR(100),
        ClonedUser NVARCHAR(100),
        PermissionState NVARCHAR(20),
        PermissionName NVARCHAR(100),
        ObjectName NVARCHAR(255),
        ObjectType NVARCHAR(50),
        GrantedBy NVARCHAR(100),
        GrantedAt DATETIME DEFAULT GETDATE()
    );
END
"@
        $ensureCmd = $destConn.CreateCommand()
        $ensureCmd.CommandText = $ensureDestSql
        $ensureCmd.ExecuteNonQuery()

        # --------- ClonedUserDetails ----------
        $detailsCommand = $sourceConn.CreateCommand()
        $detailsCommand.CommandText = "SELECT OldLogin, NewLogin, GeneratedPassword, WhoCreated, CreatedAt FROM ClonedUserDetails"
        $detailsReader = $detailsCommand.ExecuteReader()

        while ($detailsReader.Read()) {
            $columns = @()
            $values  = @()

            for ($i = 0; $i -lt $detailsReader.FieldCount; $i++) {
                $columns += $detailsReader.GetName($i)
                $val = $detailsReader.GetValue($i).ToString().Replace("'", "''")
                $values += "'$val'"
            }

            $insertQuery = "INSERT INTO ClonedUserDetails ($($columns -join ', ')) VALUES ($($values -join ', '))"
            $insertCmd = $destConn.CreateCommand()
            $insertCmd.CommandText = $insertQuery
            $insertCmd.ExecuteNonQuery()
        }
        $detailsReader.Close()

        # --------- ClonedUserPermissions ----------
        $permissionsCommand = $sourceConn.CreateCommand()
        $permissionsCommand.CommandText = "SELECT * FROM ClonedUserPermissions"
        $permissionsReader = $permissionsCommand.ExecuteReader()

        while ($permissionsReader.Read()) {
            $columns = @()
            $values  = @()

            for ($i = 0; $i -lt $permissionsReader.FieldCount; $i++) {
                $columns += $permissionsReader.GetName($i)
                $val = $permissionsReader.GetValue($i).ToString().Replace("'", "''")
                $values += "'$val'"
            }

            $insertQuery = "INSERT INTO ClonedUserPermissions ($($columns -join ', ')) VALUES ($($values -join ', '))"
            $insertCmd = $destConn.CreateCommand()
            $insertCmd.CommandText = $insertQuery
            $insertCmd.ExecuteNonQuery()
        }

        $permissionsReader.Close()
    }
    catch {
        Write-Host "Data migration error: $_" -ForegroundColor Red
        throw
    }
    finally {
        if ($sourceConn.State -eq 'Open') { $sourceConn.Close() }
        if ($destConn.State   -eq 'Open') { $destConn.Close() }
        Write-Host "Data migration completed and connections closed."
    }
    Emit-Progress 95 "MIGRATION_DONE"

    # ---------------- TRUNCATE AFTER MIGRATION (PRIMARY DB ONLY) ----------------

    $sourceConn = New-Object System.Data.SqlClient.SqlConnection($sourceConnectionString)
    $sourceConn.Open()

    try {
        $truncateCommand1 = $sourceConn.CreateCommand()
        $truncateCommand1.CommandText = "TRUNCATE TABLE ClonedUserDetails"
        $truncateCommand1.ExecuteNonQuery()

        $truncateCommand2 = $sourceConn.CreateCommand()
        $truncateCommand2.CommandText = "TRUNCATE TABLE ClonedUserPermissions"
        $truncateCommand2.ExecuteNonQuery()

        Write-Host "Tables truncated after migration!"
    }
    catch {
        Write-Host "Error while truncating: $_" -ForegroundColor Red
        throw
    }
    finally {
        if ($sourceConn.State -eq 'Open') { $sourceConn.Close() }
        Write-Host "Final connection closed."
    }
    Emit-Progress 100 "END"


# ---- POST EXECUTION SUMMARY FOR MULTIPLE MODE INVALIDS (UI ERROR MARKERS) ----
if ($mode -eq "multiple" -and $invalidPairs.Count -gt 0) {

    Write-Output "__ERROR__ Completed with errors."
    Write-Output "__ERROR__ The following logins do not exist on server '$serverInstance':"

    foreach ($p in $invalidPairs) {
        Write-Output ("__ERROR__ " + "-" + $p.OldLogin)
    }

    
    if ($outputFileFullPath -ne "") {
        Write-Output ("__ERROR__ A text file containing the invalid login names has been written to: " + $outputFileFullPath)
        Write-Output ("__ERROR__ Kindly check the file for details.")
    } else {
        Write-Output "__ERROR__ Unable to write the invalid-login file. Please check the server path or permissions."
    }
}


    exit 0
}
catch {
    Write-Host $_.Exception.Message -ForegroundColor Red

    # Attempt best-effort cleanup: drop any newly created logins for pairs we attempted
    try {
        if ($connection -and $connection.State -eq 'Open') {
            foreach ($p in $loginPairs) {
                $drop = $connection.CreateCommand()
                $escapedNew = $p.NewLogin -replace "'", "''"
                $drop.CommandText = "IF EXISTS (SELECT * FROM sys.server_principals WHERE name=N'$escapedNew') DROP LOGIN [$escapedNew];"
                try { $drop.ExecuteNonQuery() } catch {}
            }
        }
    } catch {}

    exit 1
}
finally {
    if ($connection.State -eq 'Open') {
        $connection.Close()
    }
    if ($logConnection.State -eq 'Open') {
        $logConnection.Close()
    }
    Write-Host "Primary server connection closed."
}
