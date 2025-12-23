# server_detail.py
import os
import pyodbc
from datetime import datetime
from dotenv import load_dotenv

# Load the same .env as app.py
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

INV_DB_SERVER = os.getenv("INV_DB_SERVER")
INV_DB_NAME = os.getenv("INV_DB_NAME")
INV_DB_USER = os.getenv("INV_DB_USER")
INV_DB_PASSWORD = os.getenv("INV_DB_PASSWORD")
INV_DB_TRUSTED = (os.getenv("INV_DB_TRUSTED", "YES").upper() == "YES")


def get_target_connection(server_name: str, database: str = "master"):
    """Connect to a *target* SQL Server from ServerList (not necessarily the inventory host)."""
    if not server_name:
        raise RuntimeError("server_name is required")
    if INV_DB_TRUSTED:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server_name};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
        )
    else:
        if not INV_DB_USER or not INV_DB_PASSWORD:
            raise RuntimeError("Using SQL auth but INV_DB_USER / INV_DB_PASSWORD not set")
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server_name};"
            f"DATABASE={database};"
            f"UID={INV_DB_USER};"
            f"PWD={INV_DB_PASSWORD};"
        )
    return pyodbc.connect(conn_str)


def get_server_name_by_id(server_id: int) -> str | None:
    """Lookup ServerName from dbo.ServerList in the inventory DB."""
    conn = get_inventory_connection()
    cur = conn.cursor()
    row = cur.execute("SELECT ServerName FROM dbo.ServerList WHERE ID = ?;", (server_id,)).fetchone()
    conn.close()
    return row.ServerName if row else None


def get_sql_instance_connection(database: str = "master"):
    """
    Connect to the actual SQL instance (not the inventory DB),
    defaulting to 'master'.
    """
    if INV_DB_TRUSTED:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={INV_DB_SERVER};"
            f"DATABASE={database};"
            "Trusted_Connection=yes;"
        )
    else:
        if not INV_DB_USER or not INV_DB_PASSWORD:
            raise RuntimeError("Using SQL auth but INV_DB_USER / INV_DB_PASSWORD not set")
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={INV_DB_SERVER};"
            f"DATABASE={database};"
            f"UID={INV_DB_USER};"
            f"PWD={INV_DB_PASSWORD};"
        )
    return pyodbc.connect(conn_str)


def get_server_charts():
    """
    (Optional) Chart-ready data for server dashboard.
    Not currently used by server.html, but kept for later.
    """
    conn = get_sql_instance_connection("master")
    cur = conn.cursor()

    # ---------- 1) DB size distribution ----------
    cur.execute(
        """
        ;WITH db_sizes AS (
            SELECT
                d.name AS database_name,
                CONVERT(DECIMAL(18,2),
                    SUM(mf.size) * 8.0 / 1024 / 1024
                ) AS size_gb
            FROM sys.databases AS d
            JOIN sys.master_files AS mf
              ON d.database_id = mf.database_id
            WHERE d.database_id > 4    -- user DBs only
            GROUP BY d.name
        )
        SELECT TOP (5) database_name, size_gb
        FROM db_sizes
        ORDER BY size_gb DESC;
        """
    )
    top_dbs = cur.fetchall()

    labels = []
    values = []
    total_top = 0.0

    for row in top_dbs:
        labels.append(row.database_name)
        size_val = float(row.size_gb or 0)
        values.append(size_val)
        total_top += size_val

    # Calculate "Others" bucket
    cur.execute(
        """
        SELECT
            CONVERT(DECIMAL(18,2),
                SUM(mf.size) * 8.0 / 1024 / 1024
            ) AS total_size_gb
        FROM sys.databases AS d
        JOIN sys.master_files AS mf
          ON d.database_id = mf.database_id
        WHERE d.database_id > 4;  -- user DBs only
        """
    )
    total_row = cur.fetchone()
    total_gb = float(total_row.total_size_gb or 0)
    others_gb = max(total_gb - total_top, 0.0)

    if others_gb > 0.1:  # only add if significant
        labels.append("Others")
        values.append(others_gb)

    db_size_chart = {
        "labels": labels,
        "values": values,
    }

    # ---------- 2) Recovery model distribution ----------
    cur.execute(
        """
        SELECT
            d.recovery_model_desc,
            COUNT(*) AS db_count
        FROM sys.databases AS d
        WHERE d.database_id > 4
        GROUP BY d.recovery_model_desc
        ORDER BY d.recovery_model_desc;
        """
    )
    rec_rows = cur.fetchall()
    rec_labels = []
    rec_counts = []

    for row in rec_rows:
        rec_labels.append(row.recovery_model_desc)
        rec_counts.append(int(row.db_count))

    recovery_chart = {
        "labels": rec_labels,
        "counts": rec_counts,
    }

    # ---------- 3) Drive capacity (used vs free per volume) ----------
    drive_labels = []
    drive_used = []
    drive_free = []

    try:
        cur.execute(
            """
            SELECT DISTINCT
                vs.volume_mount_point,
                CONVERT(DECIMAL(18,2), vs.total_bytes / 1024.0 / 1024 / 1024) AS total_gb,
                CONVERT(DECIMAL(18,2), vs.available_bytes / 1024.0 / 1024 / 1024) AS free_gb
            FROM sys.master_files AS mf
            CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs;
            """
        )
        for row in cur.fetchall():
            label = row.volume_mount_point.strip()
            total = float(row.total_gb or 0)
            free = float(row.free_gb or 0)
            used = max(total - free, 0.0)

            drive_labels.append(label)
            drive_used.append(used)
            drive_free.append(free)
    except Exception as ex:
        print(f"[WARN] drive chart failed: {ex}")

    drive_chart = {
        "labels": drive_labels,
        "used_gb": drive_used,
        "free_gb": drive_free,
    }

    conn.close()

    return {
        "db_size": db_size_chart,
        "recovery_model": recovery_chart,
        "drives": drive_chart,
    }


def get_inventory_connection():
    """
    Same helper as in app.py – duplicated here to avoid circular imports.
    """
    if not INV_DB_SERVER or not INV_DB_NAME:
        raise RuntimeError("INV_DB_SERVER or INV_DB_NAME is not set in .env")

    if INV_DB_TRUSTED:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={INV_DB_SERVER};"
            f"DATABASE={INV_DB_NAME};"
            "Trusted_Connection=yes;"
        )
    else:
        if not INV_DB_USER or not INV_DB_PASSWORD:
            raise RuntimeError("Using SQL auth but INV_DB_USER / INV_DB_PASSWORD not set")
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={INV_DB_SERVER};"
            f"DATABASE={INV_DB_NAME};"
            f"UID={INV_DB_USER};"
            f"PWD={INV_DB_PASSWORD};"
        )

    return pyodbc.connect(conn_str)


# ---------- BASIC SERVER ROW + DB LIST ----------

def get_server_by_id(server_id: int):
    """
    Get the ServerList + ServerInfo row for this ID.
    """
    conn = get_inventory_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            sl.ID              AS id,
            sl.ServerName      AS name,
            si.Status          AS status,
            si.SQLVersion      AS sql_version,
            si.SQLEdition      AS sql_edition,
            si.OSVersion       AS os_version,
            si.TotalDatabases  AS total_databases,
            si.TotalSizeGB     AS total_size_gb,
            si.LastScan        AS last_scan,
            si.LastRestart     AS last_restart
        FROM dbo.ServerList AS sl
        LEFT JOIN dbo.ServerInfo AS si
             ON si.ServerID = sl.ID
        WHERE sl.ID = ?
        """,
        (server_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        return None

    return {
        "id": row.id,
        "name": row.name,
        "status": row.status or "UNKNOWN",
        "sql_version": row.sql_version,
        "sql_edition": row.sql_edition,
        "os_version": row.os_version,
        "total_databases": row.total_databases or 0,
        "total_size_gb": float(row.total_size_gb or 0),
        "last_scan": row.last_scan,
        "last_restart": row.last_restart,
    }


def get_databases_for_server(server_id: int):
    """List databases for the *selected* server (not always the inventory host)."""
    server_name = get_server_name_by_id(server_id)
    if not server_name:
        return []

    conn = get_target_connection(server_name, "master")
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            name,
            state_desc        AS state,
            recovery_model_desc AS recovery_model
        FROM sys.databases
        ORDER BY database_id;
        """
    )
    dbs = []
    for row in cur.fetchall():
        dbs.append(
            {
                "name": row.name,
                "state": row.state,
                "recovery_model": row.recovery_model,
            }
        )
    conn.close()
    return dbs


# ---------- HEAVY METRICS ----------

def get_server_metrics(server_id: int):
    """Collect server-level metrics from the *selected* server using DMVs."""
    server_name = get_server_name_by_id(server_id)
    if not server_name:
        return {}

    conn = get_target_connection(server_name, "master")
    cur = conn.cursor()

    # ---- Identity & Environment ----
    cur.execute(
        """
        SELECT
            @@SERVERNAME AS instance_name,
            CAST(SERVERPROPERTY('MachineName') AS NVARCHAR(128)) AS machine_name,
            CAST(SERVERPROPERTY('ProductVersion') AS NVARCHAR(128)) AS product_version,
            CAST(SERVERPROPERTY('ProductLevel') AS NVARCHAR(128)) AS product_level,
            CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128)) AS edition,
            CAST(SERVERPROPERTY('Collation') AS NVARCHAR(128)) AS collation
        """
    )
    row = cur.fetchone()
    identity = {
        "instance_name": row.instance_name,
        "machine_name": row.machine_name,
        "sql_version": row.product_version,
        "sql_build": row.product_level,
        "edition": row.edition,
        "collation": row.collation,
    }

    # OS info (proper DMV columns)
        # OS info: derive Windows version + build from @@VERSION
    try:
        cur.execute("SELECT @@VERSION AS full_version;")
        row = cur.fetchone()
        full_version = row.full_version if row else None

        os_version = None
        os_build = None

        if full_version:
            text = full_version.replace("\r", "")

            on_idx = text.find(" on ")
            if on_idx != -1:
                os_line = text[on_idx + 4 :].strip()

                nl_idx = os_line.find("\n")
                if nl_idx != -1:
                    os_line = os_line[:nl_idx].strip()

                # ---- Short Windows version ----
                clean_os = os_line
                build_tok = clean_os.find("(Build")
                if build_tok != -1:
                    clean_os = clean_os[:build_tok].strip()

                parts = clean_os.split()
                short_os = clean_os
                if "Windows" in parts:
                    idx = parts.index("Windows")
                    if len(parts) > idx + 1:
                        short_os = f"{parts[idx]} {parts[idx+1]}"  # "Windows 10" / "Windows 11"
                os_version = short_os

                # ---- Build number ----
                build_idx = os_line.find("Build")
                if build_idx != -1:
                    build_part = os_line[build_idx + len("Build") :].strip()
                    for sep in (":", ")", "("):
                        sep_idx = build_part.find(sep)
                        if sep_idx != -1:
                            build_part = build_part[:sep_idx]
                            break
                    os_build = build_part.strip()

        identity["os_version"] = os_version
        identity["os_build"] = os_build
    except Exception:
        identity["os_version"] = None
        identity["os_build"] = None

    # NUMA / CPU basics from sys_info
    cur.execute(
        """
        SELECT
            cpu_count,
            hyperthread_ratio,
            numa_node_count,
            physical_memory_kb,
            sqlserver_start_time
        FROM sys.dm_os_sys_info;
        """
    )
    sysinfo = cur.fetchone()

    # total memory in GB
    total_memory_gb = (sysinfo.physical_memory_kb or 0) / 1024.0 / 1024.0

    # physical cores from hyperthreading ratio
    phys_cores = None
    try:
        if sysinfo.hyperthread_ratio and sysinfo.hyperthread_ratio > 0:
            phys_cores = int(sysinfo.cpu_count // sysinfo.hyperthread_ratio)
    except Exception:
        phys_cores = None

    identity["numa_nodes"] = sysinfo.numa_node_count
    identity["virtual_machine_type"] = None  # could be derived later

    # ---- SQL memory used (for reference) ----
    sql_memory_used_gb = None
    try:
        cur.execute(
            """
            SELECT physical_memory_in_use_kb
            FROM sys.dm_os_process_memory;
            """
        )
        pm = cur.fetchone()
        sql_memory_used_gb = (pm.physical_memory_in_use_kb or 0) / 1024.0 / 1024.0
    except Exception:
        sql_memory_used_gb = None

    # ---- Hardware & Capacity ----
    hardware = {
        "logical_cpus": sysinfo.cpu_count,
        "physical_cores": phys_cores,
        "total_memory_gb": total_memory_gb,
        # we'll fill sql_memory_gb (target) after reading config
        "sql_memory_gb": None,
        "sql_memory_used_gb": sql_memory_used_gb,
        "drives": [],
    }

    # Drives (where SQL has files)
    try:
        cur.execute(
            """
            ;WITH drives AS (
                SELECT DISTINCT
                    vs.volume_mount_point,
                    vs.logical_volume_name,
                    vs.total_bytes,
                    vs.available_bytes
                FROM sys.master_files AS mf
                CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs
            )
            SELECT
                volume_mount_point,
                logical_volume_name,
                total_bytes,
                available_bytes
            FROM drives;
            """
        )
        for d in cur.fetchall():
            total_gb = (d.total_bytes or 0) / 1024.0 / 1024.0 / 1024.0
            free_gb = (d.available_bytes or 0) / 1024.0 / 1024.0 / 1024.0
            free_pct = None
            if total_gb > 0:
                free_pct = round((free_gb / total_gb) * 100, 1)
            hardware["drives"].append(
                {
                    "mount_point": d.volume_mount_point,
                    "name": d.logical_volume_name,
                    "total_gb": total_gb,
                    "free_gb": free_gb,
                    "free_pct": free_pct,
                }
            )
    except Exception:
        hardware["drives"] = []

    # ---- Instance Configuration ----
    cur.execute(
        """
        SELECT
            name,
            CAST(value_in_use AS INT) AS value_in_use
        FROM sys.configurations
        WHERE name IN (
            'max server memory (MB)',
            'min server memory (MB)',
            'max degree of parallelism',
            'cost threshold for parallelism',
            'optimize for ad hoc workloads'
        );
        """
    )
    cfg_rows = cur.fetchall()
    cfg_map = {r.name: r.value_in_use for r in cfg_rows}

    # Handle max server memory (2147483647 = "unlimited")
    raw_max_mem = cfg_map.get("max server memory (MB)")
    if raw_max_mem is None:
        max_server_memory_mb = None
    elif raw_max_mem >= 2147483647:
        # treat as "all physical RAM" in MB
        max_server_memory_mb = int(total_memory_gb * 1024)
    else:
        max_server_memory_mb = raw_max_mem

    raw_min_mem = cfg_map.get("min server memory (MB)")

    # Default data/log paths
    cur.execute(
        """
        SELECT
            CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(4000)) AS data_path,
            CAST(SERVERPROPERTY('InstanceDefaultLogPath')  AS NVARCHAR(4000)) AS log_path;
        """
    )
    paths = cur.fetchone()

    # TempDB files
    cur.execute(
        """
        SELECT
            name,
            type_desc AS type,
            size * 8.0 / 1024 AS size_mb,
            is_percent_growth,
            growth,
            physical_name AS path
        FROM tempdb.sys.database_files;
        """
    )
    tempdb_files = []
    for f in cur.fetchall():
        if f.is_percent_growth:
            growth_mb = f.growth  # percent
            is_percent_growth = True
        else:
            growth_mb = (f.growth or 0) * 8.0 / 1024.0
            is_percent_growth = False
        tempdb_files.append(
            {
                "name": f.name,
                "type": f.type,
                "size_mb": float(f.size_mb),
                "is_percent_growth": is_percent_growth,
                "growth_mb": growth_mb,
                "path": f.path,
            }
        )

        if tempdb_files:
            tempdb_total_mb = sum(f["size_mb"] for f in tempdb_files)
            tempdb_total_gb = tempdb_total_mb / 1024.0
        else:
            tempdb_total_gb = None

    config = {
        "max_server_memory_mb": max_server_memory_mb,
        "min_server_memory_mb": raw_min_mem,
        "maxdop": cfg_map.get("max degree of parallelism"),
        "cost_threshold_for_parallelism": cfg_map.get(
            "cost threshold for parallelism"
        ),
        "optimize_for_adhoc": cfg_map.get("optimize for ad hoc workloads"),
        "default_data_path": paths.data_path,
        "default_log_path": paths.log_path,
        "tempdb_files": tempdb_files,
        "tempdb_total_gb": tempdb_total_gb, 
    }

    # Use max_server_memory as "SQL memory" tile (target)
    if max_server_memory_mb is not None:
        hardware["sql_memory_gb"] = max_server_memory_mb / 1024.0
    else:
        hardware["sql_memory_gb"] = None

    # ---- Database Inventory Summary ----
    cur.execute(
        """
        SELECT
            COUNT(*) AS total_databases,
            SUM(CASE WHEN database_id > 4 THEN 1 ELSE 0 END) AS user_databases,
            SUM(CASE WHEN database_id <= 4 THEN 1 ELSE 0 END) AS system_databases
        FROM sys.databases;
        """
    )
    row = cur.fetchone()
    db_summary = {
        "total_databases": row.total_databases or 0,
        "user_databases": row.user_databases or 0,
        "system_databases": row.system_databases or 0,
    }

    # by state
    cur.execute(
        """
        SELECT state_desc, COUNT(*) AS cnt
        FROM sys.databases
        GROUP BY state_desc;
        """
    )
    state_counts = {}
    for r in cur.fetchall():
        state_counts[r.state_desc] = r.cnt
    db_summary["state_counts"] = state_counts

    # by recovery model
    cur.execute(
        """
        SELECT recovery_model_desc, COUNT(*) AS cnt
        FROM sys.databases
        GROUP BY recovery_model_desc;
        """
    )
    recovery_counts = {}
    for r in cur.fetchall():
        recovery_counts[r.recovery_model_desc] = r.cnt
    db_summary["recovery_model_counts"] = recovery_counts

    # total size across user DBs
    cur.execute(
        """
        SELECT
            CONVERT(DECIMAL(18,2), SUM(size) * 8.0 / 1024 / 1024) AS total_size_gb
        FROM sys.master_files
        WHERE database_id > 4;
        """
    )
    row = cur.fetchone()
    db_summary["total_size_gb"] = float(row.total_size_gb or 0)

    # top 5 DBs by size
    cur.execute(
        """
        SELECT TOP (5)
            DB_NAME(database_id) AS database_name,
            CONVERT(DECIMAL(18,2), SUM(size) * 8.0 / 1024 / 1024) AS size_gb
        FROM sys.master_files
        WHERE database_id > 4
        GROUP BY database_id
        ORDER BY size_gb DESC;
        """
    )
    top_dbs = []
    for r in cur.fetchall():
        top_dbs.append(
            {"database_name": r.database_name, "size_gb": float(r.size_gb or 0)}
        )
    db_summary["top_databases_by_size"] = top_dbs

    # backup history from msdb
    try:
        cur.execute(
            """
            WITH last_full AS (
                SELECT database_name, MAX(backup_finish_date) AS last_full_backup
                FROM msdb.dbo.backupset
                WHERE type = 'D'
                GROUP BY database_name
            ),
            last_diff AS (
                SELECT database_name, MAX(backup_finish_date) AS last_diff_backup
                FROM msdb.dbo.backupset
                WHERE type = 'I'
                GROUP BY database_name
            ),
            last_log AS (
                SELECT database_name, MAX(backup_finish_date) AS last_log_backup
                FROM msdb.dbo.backupset
                WHERE type = 'L'
                GROUP BY database_name
            )
            SELECT
                d.name AS database_name,
                lf.last_full_backup,
                ld.last_diff_backup,
                ll.last_log_backup
            FROM sys.databases AS d
            LEFT JOIN last_full AS lf ON lf.database_name = d.name
            LEFT JOIN last_diff AS ld ON ld.database_name = d.name
            LEFT JOIN last_log AS ll ON ll.database_name = d.name
            WHERE d.database_id > 4
            ORDER BY d.name;
            """
        )
        backups = []
        for r in cur.fetchall():
            backups.append(
                {
                    "database_name": r.database_name,
                    "last_full_backup": r.last_full_backup,
                    "last_diff_backup": r.last_diff_backup,
                    "last_log_backup": r.last_log_backup,
                }
            )
        db_summary["backups"] = backups
    except Exception:
        db_summary["backups"] = []

    # ---- Object Inventory (basic counts) ----
    cur.execute(
        """
        SELECT
            SUM(CASE WHEN type = 'U' THEN 1 ELSE 0 END) AS user_tables,
            SUM(CASE WHEN type = 'V' THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN type IN ('P','PC') THEN 1 ELSE 0 END) AS procedures,
            SUM(CASE WHEN type IN ('FN','IF','TF','FS','FT') THEN 1 ELSE 0 END) AS functions
        FROM sys.objects
        WHERE is_ms_shipped = 0;
        """
    )
    row = cur.fetchone()
    object_inventory = {
        "user_tables": row.user_tables or 0,
        "views": row.views or 0,
        "procedures": row.procedures or 0,
        "functions": row.functions or 0,
    }

    cur.execute(
        """
        SELECT COUNT(*) AS login_count
        FROM sys.server_principals
        WHERE type IN ('S','U','G')
          AND name NOT LIKE '##%';
        """
    )
    row = cur.fetchone()
    object_inventory["logins"] = row.login_count or 0

    # ---------- Final pack ----------
    metrics = {
        "identity": identity,
        "hardware": hardware,
        "config": config,
        "db_summary": db_summary,
        "object_inventory": object_inventory,
    }

    conn.close()
    return metrics
