param (
    [string]$primaryServer ,
    [string]$loginName
)

# Step 1: Get list of secondary replicas
$replicaQuery = @"
SELECT r.replica_server_name
FROM sys.availability_replicas r
JOIN sys.dm_hadr_availability_replica_states rs
    ON r.replica_id = rs.replica_id
WHERE rs.role_desc = 'SECONDARY'
"@

# Build the connection string with TrustServerCertificate option
$connectionString = "Server=$primaryServer;Integrated Security=True;TrustServerCertificate=True;"

$secondaryServers = Invoke-Sqlcmd -ConnectionString $connectionString -Query $replicaQuery | Select-Object -ExpandProperty replica_server_name

if (-not $secondaryServers) {
    Write-Host "No secondary replicas found. Exiting."
    return
}

Write-Host "Found the following secondary instances:" -ForegroundColor Cyan
$secondaryServers | ForEach-Object { Write-Host " - $_" }

# Step 2: Generate the CREATE LOGIN script from primary
$loginQuery = @"
SELECT 'CREATE LOGIN [' + sp.name + '] WITH PASSWORD = ' + 
       '0x' + CONVERT(VARCHAR(MAX), sl.password_hash, 2) + ' HASHED, SID = 0x' + 
       CONVERT(VARCHAR(MAX), sl.sid, 2) + ', CHECK_POLICY = OFF' AS CreateLoginScript
FROM sys.server_principals sp
JOIN sys.sql_logins sl ON sp.sid = sl.sid
WHERE sp.name = '$loginName'
"@

$createScript = Invoke-Sqlcmd -ConnectionString $connectionString -Query $loginQuery

if (-not $createScript) {
    Write-Host "Login [$loginName] not found on [$primaryServer]. Exiting." -ForegroundColor Red
    return
}

# Step 3: Create the login on each secondary
foreach ($server in $secondaryServers) {
    try {
        Write-Host "`nCreating login on $server..." -ForegroundColor Yellow
        # Build the connection string for the secondary server
        $secondaryConnectionString = "Server=$server;Integrated Security=True;TrustServerCertificate=True;"
        Invoke-Sqlcmd -ConnectionString $secondaryConnectionString -Query $createScript.CreateLoginScript
        Write-Host "✅ Login [$loginName] created on [$server]" -ForegroundColor Green
    } catch {
        Write-Host "❌ Failed to create login on [$server]: $_" -ForegroundColor Red
    }
}