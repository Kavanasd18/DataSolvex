# db_detail.py
import os
from datetime import datetime
import pyodbc
from dotenv import load_dotenv

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
    conn = get_inventory_connection()
    cur = conn.cursor()
    row = cur.execute("SELECT ServerName FROM dbo.ServerList WHERE ID = ?;", (server_id,)).fetchone()
    conn.close()
    return row.ServerName if row else None


def get_inventory_connection():
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


# ---------- 1. Ensure db_metadata table exists ----------

def ensure_db_metadata_table():
    ddl = """
    IF NOT EXISTS (
        SELECT 1
        FROM sys.tables t
        WHERE t.name = 'db_metadata'
          AND t.schema_id = SCHEMA_ID('dbo')
    )
    BEGIN
        CREATE TABLE dbo.db_metadata (
            db_meta_id           INT IDENTITY(1,1) CONSTRAINT PK_db_metadata PRIMARY KEY,
            server_id            INT NOT NULL,     -- FK to ServerList
            database_id          INT NOT NULL,
            database_name        SYSNAME NOT NULL,
            is_system_db         BIT NOT NULL,

            state_desc           NVARCHAR(60) NOT NULL,
            user_access_desc     NVARCHAR(60) NOT NULL,
            is_read_only         BIT NOT NULL,
            is_encrypted         BIT NOT NULL,
            compatibility_level  TINYINT NOT NULL,
            collation_name       NVARCHAR(128) NULL,

            data_size_mb         DECIMAL(18,2) NULL,
            log_size_mb          DECIMAL(18,2) NULL,
            data_used_mb         DECIMAL(18,2) NULL,
            log_used_mb          DECIMAL(18,2) NULL,
            data_file_count      INT NULL,
            log_file_count       INT NULL,
            primary_data_path    NVARCHAR(260) NULL,
            log_path             NVARCHAR(260) NULL,

            recovery_model_desc  NVARCHAR(60) NOT NULL,
            last_full_backup     DATETIME NULL,
            last_diff_backup     DATETIME NULL,
            last_log_backup      DATETIME NULL,

            page_verify_option_desc NVARCHAR(60) NOT NULL,
            is_auto_close_on     BIT NOT NULL,
            is_auto_shrink_on    BIT NOT NULL,
            is_auto_create_stats_on       BIT NOT NULL,
            is_auto_update_stats_on       BIT NOT NULL,
            is_auto_update_stats_async_on BIT NOT NULL,
            is_read_committed_snapshot_on BIT NOT NULL,
            is_snapshot_isolation_on      BIT NOT NULL,

            owner_name           NVARCHAR(128) NULL,
            contains_sensitive_data BIT NULL,

            is_in_availability_group BIT NOT NULL,
            availability_group_name  NVARCHAR(128) NULL,
            is_published_for_replication BIT NOT NULL,
            is_subscribed_for_replication BIT NOT NULL,

            last_dbcc_checkdb   DATETIME NULL,
            last_user_access    DATETIME NULL,

            collected_at        DATETIME NOT NULL DEFAULT (GETDATE())
        );

        ALTER TABLE dbo.db_metadata
        ADD CONSTRAINT FK_db_metadata_ServerList
            FOREIGN KEY (server_id) REFERENCES dbo.ServerList(ID);
    END;
    """

    conn = get_inventory_connection()
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    conn.close()



# ---------- 2. Snapshot metadata for ONE database ----------

def snapshot_db_metadata(server_id: int, database_name: str):
    """
    Capture metadata for a single database on this instance into dbo.db_metadata.
    - If row exists for (server_id, database_id), UPDATE.
    - Else, INSERT.
    """

    # Inventory DB connection (for reading ServerList + writing dbo.db_metadata)
    inv_conn = get_inventory_connection()
    inv_cur = inv_conn.cursor()

    server_name = get_server_name_by_id(server_id)
    if not server_name:
        inv_conn.close()
        return

    # Target server connection (for reading sys.* metadata from that server)
    target_conn = get_target_connection(server_name, "master")
    tcur = target_conn.cursor()

    # Basic row from sys.databases
    tcur.execute(
        """
        SELECT
            d.database_id,
            d.name                  AS database_name,
            CASE WHEN d.database_id <= 4 THEN 1 ELSE 0 END AS is_system_db,
            d.state_desc,
            d.user_access_desc,
            d.is_read_only,
            d.is_encrypted,
            d.compatibility_level,
            d.collation_name,
            d.recovery_model_desc,
            d.page_verify_option_desc,
            d.is_auto_close_on,
            d.is_auto_shrink_on,
            d.is_auto_create_stats_on,
            d.is_auto_update_stats_on,
            d.is_auto_update_stats_async_on,
            d.is_read_committed_snapshot_on,
            d.snapshot_isolation_state,
            d.snapshot_isolation_state_desc,
            SUSER_SNAME(d.owner_sid) AS owner_name,
            d.is_published           AS is_published_for_replication,
            d.is_subscribed          AS is_subscribed_for_replication
        FROM sys.databases AS d
        WHERE d.name = ?
        """,
        (database_name,),
    )
    db_row = tcur.fetchone()
    if not db_row:
        target_conn.close()
        inv_conn.close()
        raise ValueError(f"Database '{database_name}' not found on this instance.")

    database_id = db_row.database_id

    # snapshot_isolation_state -> BIT-style flag
    snapshot_state = getattr(db_row, "snapshot_isolation_state", 0) or 0
    is_snapshot_isolation_on = 1 if snapshot_state == 1 else 0

    # Size info from sys.master_files
    tcur.execute(
        """
        SELECT
            CONVERT(DECIMAL(18,2),
                SUM(CASE WHEN mf.type_desc = 'ROWS' THEN mf.size END) * 8.0 / 1024/1024
            ) AS data_size_mb,
            CONVERT(DECIMAL(18,2),
                SUM(CASE WHEN mf.type_desc = 'LOG'  THEN mf.size END) * 8.0 / 1024/1024
            ) AS log_size_mb,
            SUM(CASE WHEN mf.type_desc = 'ROWS' THEN 1 ELSE 0 END) AS data_file_count,
            SUM(CASE WHEN mf.type_desc = 'LOG'  THEN 1 ELSE 0 END) AS log_file_count,
            MAX(CASE WHEN mf.type_desc = 'ROWS' THEN mf.physical_name END) AS primary_data_path,
            MAX(CASE WHEN mf.type_desc = 'LOG'  THEN mf.physical_name END) AS log_path
        FROM sys.master_files AS mf
        WHERE DB_NAME(mf.database_id) = ?;
        """,
        (database_name,),
    )
    size_row = tcur.fetchone()
    data_size_mb = size_row.data_size_mb or 0
    log_size_mb = size_row.log_size_mb or 0
    data_file_count = size_row.data_file_count or 0
    log_file_count = size_row.log_file_count or 0
    primary_data_path = size_row.primary_data_path
    log_path = size_row.log_path

    data_used_mb = None
    log_used_mb = None

    # Backup history (msdb)
    try:
        tcur.execute(
            """
            WITH last_full AS (
                SELECT MAX(backup_finish_date) AS last_full_backup
                FROM msdb.dbo.backupset
                WHERE type = 'D'
                  AND database_name = ?
            ),
            last_diff AS (
                SELECT MAX(backup_finish_date) AS last_diff_backup
                FROM msdb.dbo.backupset
                WHERE type = 'I'
                  AND database_name = ?
            ),
            last_log AS (
                SELECT MAX(backup_finish_date) AS last_log_backup
                FROM msdb.dbo.backupset
                WHERE type = 'L'
                  AND database_name = ?
            )
            SELECT
                lf.last_full_backup,
                ld.last_diff_backup,
                ll.last_log_backup
            FROM last_full lf
            CROSS JOIN last_diff ld
            CROSS JOIN last_log ll;
            """,
            (database_name, database_name, database_name),
        )
        b = tcur.fetchone()
        if b:
            last_full_backup = b.last_full_backup
            last_diff_backup = b.last_diff_backup
            last_log_backup = b.last_log_backup
        else:
            last_full_backup = None
            last_diff_backup = None
            last_log_backup = None
    except Exception:
        last_full_backup = None
        last_diff_backup = None
        last_log_backup = None

    # AG participation
    is_in_availability_group = 0
    availability_group_name = None
    try:
        tcur.execute(
            """
            SELECT TOP (1)
                1 AS is_in_ag,
                ag.name AS ag_name
            FROM sys.databases AS d
            INNER JOIN sys.dm_hadr_database_replica_states AS drs
                ON d.database_id = drs.database_id
            INNER JOIN sys.availability_groups AS ag
                ON drs.group_id = ag.group_id
            WHERE d.name = ?;
            """,
            (database_name,),
        )
        ag_row = tcur.fetchone()
        if ag_row:
            is_in_availability_group = ag_row.is_in_ag
            availability_group_name = ag_row.ag_name
    except Exception:
        pass

    # Last user access
    last_user_access = None
    try:
        tcur.execute(
            """
            SELECT MAX(last_access) AS last_access
            FROM (
                SELECT MAX(last_user_seek)   AS last_access
                FROM sys.dm_db_index_usage_stats
                WHERE database_id = ?
                UNION ALL
                SELECT MAX(last_user_scan)
                FROM sys.dm_db_index_usage_stats
                WHERE database_id = ?
                UNION ALL
                SELECT MAX(last_user_lookup)
                FROM sys.dm_db_index_usage_stats
                WHERE database_id = ?
                UNION ALL
                SELECT MAX(last_user_update)
                FROM sys.dm_db_index_usage_stats
                WHERE database_id = ?
            ) AS x;
            """,
            (database_id, database_id, database_id, database_id),
        )
        lu = tcur.fetchone()
        if lu and lu.last_access:
            last_user_access = lu.last_access
    except Exception:
        last_user_access = None

    last_dbcc_checkdb = None
    contains_sensitive_data = None

    # Upsert into db_metadata
    inv_cur.execute(
        """
        SELECT db_meta_id
        FROM dbo.db_metadata
        WHERE server_id = ?
          AND database_id = ?;
        """,
        (server_id, database_id),
    )
    existing = inv_cur.fetchone()

    if existing:
        # UPDATE
        inv_cur.execute(
            """
            UPDATE dbo.db_metadata
            SET
                database_name        = ?,
                is_system_db         = ?,

                state_desc           = ?,
                user_access_desc     = ?,
                is_read_only         = ?,
                is_encrypted         = ?,
                compatibility_level  = ?,
                collation_name       = ?,

                data_size_mb         = ?,
                log_size_mb          = ?,
                data_used_mb         = ?,
                log_used_mb          = ?,
                data_file_count      = ?,
                log_file_count       = ?,
                primary_data_path    = ?,
                log_path             = ?,

                recovery_model_desc  = ?,
                last_full_backup     = ?,
                last_diff_backup     = ?,
                last_log_backup      = ?,

                page_verify_option_desc = ?,
                is_auto_close_on     = ?,
                is_auto_shrink_on    = ?,
                is_auto_create_stats_on       = ?,
                is_auto_update_stats_on       = ?,
                is_auto_update_stats_async_on = ?,
                is_read_committed_snapshot_on = ?,
                is_snapshot_isolation_on      = ?,

                owner_name           = ?,
                contains_sensitive_data = ?,

                is_in_availability_group = ?,
                availability_group_name  = ?,
                is_published_for_replication = ?,
                is_subscribed_for_replication = ?,

                last_dbcc_checkdb   = ?,
                last_user_access    = ?,
                collected_at        = GETDATE()
            WHERE server_id   = ?
              AND database_id = ?;
            """,
            (
                db_row.database_name,
                db_row.is_system_db,

                db_row.state_desc,
                db_row.user_access_desc,
                db_row.is_read_only,
                db_row.is_encrypted,
                db_row.compatibility_level,
                db_row.collation_name,

                data_size_mb,
                log_size_mb,
                data_used_mb,
                log_used_mb,
                data_file_count,
                log_file_count,
                primary_data_path,
                log_path,

                db_row.recovery_model_desc,
                last_full_backup,
                last_diff_backup,
                last_log_backup,

                db_row.page_verify_option_desc,
                db_row.is_auto_close_on,
                db_row.is_auto_shrink_on,
                db_row.is_auto_create_stats_on,
                db_row.is_auto_update_stats_on,
                db_row.is_auto_update_stats_async_on,
                db_row.is_read_committed_snapshot_on,
                is_snapshot_isolation_on,

                db_row.owner_name,
                contains_sensitive_data,

                is_in_availability_group,
                availability_group_name,
                db_row.is_published_for_replication,
                db_row.is_subscribed_for_replication,

                last_dbcc_checkdb,
                last_user_access,
                server_id,
                database_id,
            ),
        )
    else:
        # INSERT
        inv_cur.execute(
            """
            INSERT INTO dbo.db_metadata (
                server_id,
                database_id,
                database_name,
                is_system_db,

                state_desc,
                user_access_desc,
                is_read_only,
                is_encrypted,
                compatibility_level,
                collation_name,

                data_size_mb,
                log_size_mb,
                data_used_mb,
                log_used_mb,
                data_file_count,
                log_file_count,
                primary_data_path,
                log_path,

                recovery_model_desc,
                last_full_backup,
                last_diff_backup,
                last_log_backup,

                page_verify_option_desc,
                is_auto_close_on,
                is_auto_shrink_on,
                is_auto_create_stats_on,
                is_auto_update_stats_on,
                is_auto_update_stats_async_on,
                is_read_committed_snapshot_on,
                is_snapshot_isolation_on,

                owner_name,
                contains_sensitive_data,

                is_in_availability_group,
                availability_group_name,
                is_published_for_replication,
                is_subscribed_for_replication,

                last_dbcc_checkdb,
                last_user_access,
                collected_at
            )
            VALUES (
                ?,  -- server_id
                ?,  -- database_id
                ?,  -- database_name
                ?,  -- is_system_db

                ?,  -- state_desc
                ?,  -- user_access_desc
                ?,  -- is_read_only
                ?,  -- is_encrypted
                ?,  -- compatibility_level
                ?,  -- collation_name

                ?,  -- data_size_mb
                ?,  -- log_size_mb
                ?,  -- data_used_mb
                ?,  -- log_used_mb
                ?,  -- data_file_count
                ?,  -- log_file_count
                ?,  -- primary_data_path
                ?,  -- log_path

                ?,  -- recovery_model_desc
                ?,  -- last_full_backup
                ?,  -- last_diff_backup
                ?,  -- last_log_backup

                ?,  -- page_verify_option_desc
                ?,  -- is_auto_close_on
                ?,  -- is_auto_shrink_on
                ?,  -- is_auto_create_stats_on
                ?,  -- is_auto_update_stats_on
                ?,  -- is_auto_update_stats_async_on
                ?,  -- is_read_committed_snapshot_on
                ?,  -- is_snapshot_isolation_on

                ?,  -- owner_name
                ?,  -- contains_sensitive_data

                ?,  -- is_in_availability_group
                ?,  -- availability_group_name
                ?,  -- is_published_for_replication
                ?,  -- is_subscribed_for_replication

                ?,  -- last_dbcc_checkdb
                ?,  -- last_user_access
                GETDATE()
            );
            """,
            (
                server_id,
                database_id,
                db_row.database_name,
                db_row.is_system_db,

                db_row.state_desc,
                db_row.user_access_desc,
                db_row.is_read_only,
                db_row.is_encrypted,
                db_row.compatibility_level,
                db_row.collation_name,

                data_size_mb,
                log_size_mb,
                data_used_mb,
                log_used_mb,
                data_file_count,
                log_file_count,
                primary_data_path,
                log_path,

                db_row.recovery_model_desc,
                last_full_backup,
                last_diff_backup,
                last_log_backup,

                db_row.page_verify_option_desc,
                db_row.is_auto_close_on,
                db_row.is_auto_shrink_on,
                db_row.is_auto_create_stats_on,
                db_row.is_auto_update_stats_on,
                db_row.is_auto_update_stats_async_on,
                db_row.is_read_committed_snapshot_on,
                is_snapshot_isolation_on,

                db_row.owner_name,
                contains_sensitive_data,

                is_in_availability_group,
                availability_group_name,
                db_row.is_published_for_replication,
                db_row.is_subscribed_for_replication,

                last_dbcc_checkdb,
                last_user_access,
            ),
        )

    inv_conn.commit()
    target_conn.close()
    inv_conn.close()


# ---------- 3. Read metadata back for the dashboard ----------

def get_db_metadata(server_id: int, database_name: str):
    conn = get_inventory_connection()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT TOP (1) *
        FROM dbo.db_metadata
        WHERE server_id = ?
          AND database_name = ?
        ORDER BY collected_at DESC;
        """,
        (server_id, database_name),
    )
    row = cur.fetchone()
    conn.close()
    if not row:
        return None
    
    rcsi_on = bool(row.is_read_committed_snapshot_on)
    snapshot_on = bool(row.is_snapshot_isolation_on)

    if rcsi_on and snapshot_on:
        isolation_desc = "READ COMMITTED SNAPSHOT + SNAPSHOT"
    elif snapshot_on:
        isolation_desc = "SNAPSHOT ISOLATION"
    elif rcsi_on:
        isolation_desc = "READ COMMITTED SNAPSHOT (RCSI)"
    else:
        isolation_desc = "READ COMMITTED (default)"

    if not row:
        return None

    db_meta = {
        "db_meta_id": row.db_meta_id,
        "server_id": row.server_id,
        "database_id": row.database_id,
        "database_name": row.database_name,
        "is_system_db": row.is_system_db,

        "state_desc": row.state_desc,
        "user_access_desc": row.user_access_desc,
        "is_read_only": row.is_read_only,
        "is_encrypted": row.is_encrypted,
        "compatibility_level": row.compatibility_level,
        "collation_name": row.collation_name,

        "data_size_mb": float(row.data_size_mb) if row.data_size_mb is not None else None,
        "log_size_mb": float(row.log_size_mb) if row.log_size_mb is not None else None,
        "data_used_mb": float(row.data_used_mb) if row.data_used_mb is not None else None,
        "log_used_mb": float(row.log_used_mb) if row.log_used_mb is not None else None,
        "data_file_count": row.data_file_count,
        "log_file_count": row.log_file_count,
        "primary_data_path": row.primary_data_path,
        "log_path": row.log_path,

        "recovery_model_desc": row.recovery_model_desc,
        "last_full_backup": row.last_full_backup,
        "last_diff_backup": row.last_diff_backup,
        "last_log_backup": row.last_log_backup,

        "page_verify_option_desc": row.page_verify_option_desc,
        "is_auto_close_on": row.is_auto_close_on,
        "is_auto_shrink_on": row.is_auto_shrink_on,
        "is_auto_create_stats_on": row.is_auto_create_stats_on,
        "is_auto_update_stats_on": row.is_auto_update_stats_on,
        "is_auto_update_stats_async_on": row.is_auto_update_stats_async_on,
        "is_read_committed_snapshot_on": row.is_read_committed_snapshot_on,
        "is_snapshot_isolation_on": row.is_snapshot_isolation_on,
        "isolation_level_desc": isolation_desc,
        "owner_name": row.owner_name,
        "contains_sensitive_data": row.contains_sensitive_data,

        "is_in_availability_group": row.is_in_availability_group,
        "availability_group_name": row.availability_group_name,
        "is_published_for_replication": row.is_published_for_replication,
        "is_subscribed_for_replication": row.is_subscribed_for_replication,

        "last_dbcc_checkdb": row.last_dbcc_checkdb,
        "last_user_access": row.last_user_access,
        "collected_at": row.collected_at,
    }
    compat_map = {
        80: "SQL Server 2000",
        90: "SQL Server 2005",
        100: "SQL Server 2008 / 2008 R2",
        110: "SQL Server 2012",
        120: "SQL Server 2014",
        130: "SQL Server 2016",
        140: "SQL Server 2017",
        150: "SQL Server 2019 ",
        160: "SQL Server 2022 ",
    }

    level = db_meta.get("compatibility_level")
    db_meta["compatibility_desc"] = compat_map.get(level, None)

    # --- RPO calculation (minutes since last log backup) ---
    from datetime import datetime

    rpo_minutes = None
    last_log = db_meta.get("last_log_backup")
    if last_log and isinstance(last_log, datetime):
        # If the datetime is naive, treat it as server local; otherwise respect tzinfo
        now = datetime.now(tz=last_log.tzinfo) if last_log.tzinfo else datetime.now()
        delta = now - last_log
        rpo_minutes = max(0, int(delta.total_seconds() / 60))

    db_meta["rpo_minutes"] = rpo_minutes

    return db_meta


def get_db_object_summary(database_name: str, server_name: str | None = None):
    """
    Connect directly to the target database and return:
      - user table / view / proc / function counts
      - largest user table (name, rows, size MB)
    """
    # Build a connection string to the TARGET database (not the inventory DB)
    if INV_DB_TRUSTED:
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server_name or INV_DB_SERVER};"
            f"DATABASE={database_name};"
            "Trusted_Connection=yes;"
        )
    else:
        if not INV_DB_USER or not INV_DB_PASSWORD:
            raise RuntimeError("Using SQL auth but INV_DB_USER / INV_DB_PASSWORD not set")
        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={server_name or INV_DB_SERVER};"
            f"DATABASE={database_name};"
            f"UID={INV_DB_USER};"
            f"PWD={INV_DB_PASSWORD};"
        )

    conn = None
    try:
        conn = pyodbc.connect(conn_str)
        cur = conn.cursor()

        # 1) Object counts (user objects only)
        cur.execute(
            """
            SELECT
                (SELECT COUNT(*) FROM sys.tables      WHERE is_ms_shipped = 0) AS user_tables,
                (SELECT COUNT(*) FROM sys.views       WHERE is_ms_shipped = 0) AS user_views,
                (SELECT COUNT(*) FROM sys.procedures  WHERE is_ms_shipped = 0) AS user_procs,
                (SELECT COUNT(*)
                 FROM sys.objects
                 WHERE is_ms_shipped = 0
                   AND type IN ('FN','IF','TF')) AS user_functions,
                (SELECT COUNT(*)
                 FROM sys.triggers
                 WHERE is_ms_shipped = 0
                   AND parent_class_desc = 'DATABASE') AS db_level_triggers;
            """
        )
        counts = cur.fetchone()

        table_count = counts.user_tables if counts and counts.user_tables is not None else 0
        view_count  = counts.user_views  if counts and counts.user_views  is not None else 0
        proc_count  = counts.user_procs  if counts and counts.user_procs  is not None else 0
        func_count  = counts.user_functions if counts and counts.user_functions is not None else 0
        db_trigger_count = counts.db_level_triggers or 0

        # 2) Largest user table by used space
        cur.execute(
            """
            SELECT TOP (1)
                t.name AS table_name,
                SUM(ps.row_count) AS total_rows,
                CONVERT(DECIMAL(18,2), SUM(ps.used_page_count) * 8.0 / 1024/1024) AS used_mb
            FROM sys.tables AS t
            JOIN sys.dm_db_partition_stats AS ps
              ON t.object_id = ps.object_id
             AND ps.index_id IN (0,1)          -- heap or clustered index
            WHERE t.is_ms_shipped = 0
            GROUP BY t.name
            ORDER BY used_mb DESC, total_rows DESC;
            """
        )
        largest = cur.fetchone()

        largest_table_name = largest.table_name if largest else None
        largest_table_rows = int(largest.total_rows) if largest and largest.total_rows is not None else None
        largest_table_mb   = float(largest.used_mb) if largest and largest.used_mb is not None else None

        return {
            "table_count": table_count,
            "view_count": view_count,
            "proc_count": proc_count,
            "func_count": func_count,
            "db_trigger_count": db_trigger_count,
            "largest_table_name": largest_table_name,
            "largest_table_rows": largest_table_rows,
            "largest_table_mb": largest_table_mb,
        }

    except Exception as ex:
        print(f"[WARN] get_db_object_summary failed for {database_name}: {ex}")
        return {
            "table_count": 0,
            "view_count": 0,
            "proc_count": 0,
            "func_count": 0,
            "db_trigger_count": 0,
            "largest_table_name": None,
            "largest_table_rows": 0,
            "largest_table_mb": 0.0,
        }
    finally:
        if conn is not None:
            conn.close()
