# app.py
import os
import io
import csv 
from datetime import datetime

import pyodbc
from flask import (
    Flask,
    render_template,
    redirect,
    url_for,
    abort,
    request,
    Response,
    make_response,
    send_file,
)
from dotenv import load_dotenv

from .objects_detail import get_object_metadata
from .db_detail import (
    ensure_db_metadata_table,
    snapshot_db_metadata,
    get_db_metadata,
    get_db_object_summary,
)
from .server_detail import (
    get_server_by_id,
    get_server_metrics,
    get_databases_for_server,
)
from .db_objects import get_db_user_count


# ---------- ENV CONFIG ----------
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))
ensure_db_metadata_table()

INV_DB_SERVER = os.getenv("INV_DB_SERVER")
INV_DB_NAME = os.getenv("INV_DB_NAME")
INV_DB_USER = os.getenv("INV_DB_USER")
INV_DB_PASSWORD = os.getenv("INV_DB_PASSWORD")
INV_DB_TRUSTED = (os.getenv("INV_DB_TRUSTED", "YES").upper() == "YES")

app = Flask(__name__)


# ---------- PLAN AVAILABILITY (for disabling Download Plan button) ----------

def has_cached_proc_plan(conn, db_name: str, proc_full_name: str) -> bool:
    """True when a cached procedure plan is available for download.

    This checks sys.dm_exec_procedure_stats, which only has rows after the
    procedure has executed and while its plan is still in cache.
    """

    if not proc_full_name:
        return False

    try:
        cur = conn.cursor()
        # Ensure OBJECT_ID resolves in the right DB
        cur.execute(f"USE [{db_name.replace(']', ']]')}]")
        row = cur.execute(
            """
            SELECT TOP (1) 1
            FROM sys.dm_exec_procedure_stats AS ps
            WHERE ps.object_id = OBJECT_ID(?);
            """,
            proc_full_name,
        ).fetchone()
        return row is not None
    except Exception:
        # Be conservative: if check fails, disable the button
        return False


# ---------- DB CONNECTION (INVENTORY DB) ----------

def get_inventory_connection():
    """
    Connect to the inventory database (where ServerList / ServerInfo live).
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

##change
def get_sql_connection(server_name: str, database: str = "master"):
    """Connect to a *target* SQL Server instance from ServerList."""
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

# ---------- SNAPSHOT SQL (POPULATE ServerInfo FOR THIS SERVER) ----------

SNAPSHOT_SQL = """
DECLARE @ServerName SYSNAME = @@SERVERNAME;
DECLARE @ServerID   INT;

IF NOT EXISTS (SELECT 1 FROM dbo.ServerList WHERE ServerName = @ServerName)
BEGIN
    INSERT INTO dbo.ServerList (ServerName)
    VALUES (@ServerName);
END;

SELECT TOP (1) @ServerID = ID
FROM dbo.ServerList
WHERE ServerName = @ServerName
ORDER BY ID;

WITH ServerSnapshot AS (
    SELECT
        @ServerID AS ServerID,
        'ONLINE' AS Status,
        CAST(
            LEFT(@@VERSION, CHARINDEX(CHAR(10), @@VERSION + CHAR(10)) - 1)
            AS NVARCHAR(256)
        ) AS SQLVersion,
        CAST(SERVERPROPERTY('Edition')        AS NVARCHAR(128)) AS SQLEdition,
        (SELECT TOP (1) windows_release 
         FROM sys.dm_os_windows_info) AS OSVersion,
        (SELECT CONVERT(DATETIME2, sqlserver_start_time)
         FROM sys.dm_os_sys_info) AS LastRestart,
        (SELECT COUNT(*)
         FROM sys.databases
         WHERE database_id > 4) AS TotalDatabases,
        (SELECT CONVERT(DECIMAL(18,2), SUM(size) * 8.0 / 1024 / 1024)
         FROM sys.master_files
         WHERE database_id > 4) AS TotalSizeGB,
        SYSDATETIME() AS LastScan,
        SYSDATETIME() AS LastUpdated
)
MERGE dbo.ServerInfo AS tgt
USING ServerSnapshot AS src
      ON tgt.ServerID = src.ServerID
WHEN MATCHED THEN
    UPDATE SET
        tgt.Status         = src.Status,
        tgt.SQLVersion     = src.SQLVersion,
        tgt.SQLEdition     = src.SQLEdition,
        tgt.OSVersion      = src.OSVersion,
        tgt.LastRestart    = src.LastRestart,
        tgt.TotalDatabases = src.TotalDatabases,
        tgt.TotalSizeGB    = src.TotalSizeGB,
        tgt.LastScan       = src.LastScan,
        tgt.LastUpdated    = src.LastUpdated
WHEN NOT MATCHED BY TARGET THEN
    INSERT (
        ServerID,
        Status,
        SQLVersion,
        SQLEdition,
        OSVersion,
        LastRestart,
        TotalDatabases,
        TotalSizeGB,
        LastScan,
        LastUpdated
    )
    VALUES (
        src.ServerID,
        src.Status,
        src.SQLVersion,
        src.SQLEdition,
        src.OSVersion,
        src.LastRestart,
        src.TotalDatabases,
        src.TotalSizeGB,
        src.LastScan,
        src.LastUpdated
    );
"""
##change
def refresh_all_server_info():
    """
    Run the snapshot SQL on the *SQL instance* that hosts the inventory DB.
    This ensures we have an up-to-date ServerInfo row for @@SERVERNAME.
    """
    conn = get_inventory_connection()
    cur = conn.cursor()
    cur.execute(SNAPSHOT_SQL)
    conn.commit()
    conn.close()

def refresh_all_server_info():
    """Refresh dbo.ServerInfo for *every* server in dbo.ServerList.

    Previously the app refreshed only the inventory host (central server), so every
    server card (and downstream clicks) effectively showed central-server data.
    """
    inv_conn = get_inventory_connection()
    inv_cur = inv_conn.cursor()

    servers = inv_cur.execute(
        "SELECT ID, ServerName FROM dbo.ServerList ORDER BY ServerName;"
    ).fetchall()

    for s in servers:
        server_id = int(s.ID)
        server_name = str(s.ServerName)

        status = "ONLINE"
        sql_version = None
        sql_edition = None
        os_version = None
        last_restart = None
        total_databases = 0
        total_size_gb = 0.0

        try:
            tconn = get_sql_connection(server_name, "master")
            tcur = tconn.cursor()

            row = tcur.execute(
                "SELECT CAST(LEFT(@@VERSION, CHARINDEX(CHAR(10), @@VERSION + CHAR(10)) - 1) AS NVARCHAR(256));"
            ).fetchone()
            sql_version = row[0] if row else None

            row = tcur.execute("SELECT CAST(SERVERPROPERTY('Edition') AS NVARCHAR(128));").fetchone()
            sql_edition = row[0] if row else None

            try:
                row = tcur.execute("SELECT TOP (1) windows_release FROM sys.dm_os_windows_info;").fetchone()
                os_version = row[0] if row else None
            except Exception:
                os_version = None

            row = tcur.execute("SELECT CONVERT(DATETIME2, sqlserver_start_time) FROM sys.dm_os_sys_info;").fetchone()
            last_restart = row[0] if row else None

            row = tcur.execute("SELECT COUNT(*) FROM sys.databases WHERE database_id > 4;").fetchone()
            total_databases = int(row[0] or 0) if row else 0

            row = tcur.execute(
                "SELECT CONVERT(DECIMAL(18,2), SUM(size) * 8.0 / 1024 / 1024) FROM sys.master_files WHERE database_id > 4;"
            ).fetchone()
            total_size_gb = float(row[0] or 0) if row else 0.0

            tconn.close()
        except Exception:
            status = "OFFLINE"

        inv_cur.execute(
            """
            MERGE dbo.ServerInfo AS tgt
            USING (SELECT ? AS ServerID) AS src
              ON tgt.ServerID = src.ServerID
            WHEN MATCHED THEN
                UPDATE SET
                    Status         = ?,
                    SQLVersion     = ?,
                    SQLEdition     = ?,
                    OSVersion      = ?,
                    LastRestart    = ?,
                    TotalDatabases = ?,
                    TotalSizeGB    = ?,
                    LastScan       = SYSDATETIME(),
                    LastUpdated    = SYSDATETIME()
            WHEN NOT MATCHED THEN
                INSERT (ServerID, Status, SQLVersion, SQLEdition, OSVersion, LastRestart, TotalDatabases, TotalSizeGB, LastScan, LastUpdated)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, SYSDATETIME(), SYSDATETIME());
            """,
            (
                server_id,
                status, sql_version, sql_edition, os_version, last_restart, total_databases, total_size_gb,
                server_id, status, sql_version, sql_edition, os_version, last_restart, total_databases, total_size_gb,
            ),
        )

    inv_conn.commit()
    inv_conn.close()    


def get_servers(sql_version=None, windows_version=None):
    """
    Get all servers for the home page cards from ServerList + ServerInfo,
    with optional filters on SQLVersion and OSVersion (Windows version).
    """
    conn = get_inventory_connection()
    cur = conn.cursor()

    query = """
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
        WHERE 1 = 1
    """

    params = []

    if sql_version:
        query += " AND si.SQLVersion LIKE ?"
        params.append(f"%{sql_version}%")

    if windows_version:
        # OSVersion holds the Windows release string (what we treat as 'Windows version')
        query += " AND si.OSVersion LIKE ?"
        params.append(f"%{windows_version}%")

    query += " ORDER BY sl.ServerName;"

    cur.execute(query, params)

    servers = []
    for row in cur.fetchall():
        servers.append(
            {
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
        )

    conn.close()
    return servers


# ---------- DB OBJECT INVENTORY HELPERS ----------

def get_db_objects_summary_and_lists(server_name: str, db_name: str):
    """
    Returns:
      objects_summary = {
        "tables": int,
        "views": int,
        "procedures": int,
        "functions": int,
      }

      objects_by_type = {
        "TABLE":     [ { "full_name": "schema.table" }, ... ],
        "VIEW":      [ { "full_name": "schema.view" }, ... ],
        "PROCEDURE": [ { "full_name": "schema.proc" }, ... ],
        "FUNCTION":  [ { "full_name": "schema.func" }, ... ],
      }

    Uses the same SQL instance as the inventory DB and switches to [db_name].
    """
    conn = get_sql_connection(server_name, db_name)
    cur = conn.cursor()

    # ---- Summary counts ----
    cur.execute(
        """
        SELECT 
            SUM(CASE WHEN type = 'U'                          THEN 1 ELSE 0 END) AS tables,
            SUM(CASE WHEN type = 'V'                          THEN 1 ELSE 0 END) AS views,
            SUM(CASE WHEN type IN ('P','X')                   THEN 1 ELSE 0 END) AS procedures,
            SUM(CASE WHEN type IN ('FN','IF','TF','FS','FT')  THEN 1 ELSE 0 END) AS functions
        FROM sys.objects
        WHERE is_ms_shipped = 0
          AND type IN ('U','V','P','X','FN','IF','TF','FS','FT');
        """
    )
    r = cur.fetchone()
    objects_summary = {
        "tables": int(r.tables or 0),
        "views": int(r.views or 0),
        "procedures": int(r.procedures or 0),
        "functions": int(r.functions or 0),
    }

    cur.execute(
        """
        SELECT COUNT(*) AS table_triggers
        FROM sys.triggers
        WHERE is_ms_shipped = 0
          AND parent_class_desc = 'OBJECT_OR_COLUMN';
        """
    )
    r_trig = cur.fetchone()
    objects_summary["table_triggers"] = int(r_trig.table_triggers or 0)

    # ---- Lists per type ----
    objects_by_type = {
        "TABLE": [],
        "VIEW": [],
        "PROCEDURE": [],
        "FUNCTION": [],
    }

    # Tables
    cur.execute(
        """
        SELECT s.name AS schema_name, t.name AS object_name
        FROM sys.tables AS t
        INNER JOIN sys.schemas AS s
            ON t.schema_id = s.schema_id
        WHERE t.is_ms_shipped = 0
        ORDER BY s.name, t.name;
        """
    )
    for row in cur.fetchall():
        objects_by_type["TABLE"].append(
            {"full_name": f"{row.schema_name}.{row.object_name}"}
        )

    # Views
    cur.execute(
        """
        SELECT s.name AS schema_name, v.name AS object_name
        FROM sys.views AS v
        INNER JOIN sys.schemas AS s
            ON v.schema_id = s.schema_id
        WHERE v.is_ms_shipped = 0
        ORDER BY s.name, v.name;
        """
    )
    for row in cur.fetchall():
        objects_by_type["VIEW"].append(
            {"full_name": f"{row.schema_name}.{row.object_name}"}
        )

    # Stored procedures (P, X)
    cur.execute(
        """
        SELECT s.name AS schema_name, p.name AS object_name
        FROM sys.procedures AS p
        INNER JOIN sys.schemas AS s
            ON p.schema_id = s.schema_id
        WHERE p.is_ms_shipped = 0
        ORDER BY s.name, p.name;
        """
    )
    for row in cur.fetchall():
        objects_by_type["PROCEDURE"].append(
            {"full_name": f"{row.schema_name}.{row.object_name}"}
        )

    # Functions
    cur.execute(
        """
        SELECT s.name AS schema_name, o.name AS object_name
        FROM sys.objects AS o
        INNER JOIN sys.schemas AS s
            ON o.schema_id = s.schema_id
        WHERE o.is_ms_shipped = 0
          AND o.type IN ('FN','IF','TF','FS','FT')
        ORDER BY s.name, o.name;
        """
    )
    for row in cur.fetchall():
        objects_by_type["FUNCTION"].append(
            {"full_name": f"{row.schema_name}.{row.object_name}"}
        )

    conn.close()
    return objects_summary, objects_by_type

# ---------- TEMPLATE FILTERS ----------

@app.template_filter("fmt_dt")
def format_datetime(value):
    if not value:
        return "N/A"
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value)


# ---------- ROUTES ----------

@app.route("/")
def index():
    # Refresh local server snapshot each time (for now).
    refresh_all_server_info()

    # Read filters from query string
    sql_version = (request.args.get("sql_version") or "").strip()
    windows_version = (request.args.get("windows_version") or "").strip()

    servers = get_servers(
        sql_version=sql_version or None,
        windows_version=windows_version or None,
    )

    return render_template(
        "index.html",
        servers=servers,
        sql_version=sql_version,
        windows_version=windows_version,
    )


@app.route("/refresh")
def refresh():
    refresh_all_server_info()
    return redirect(url_for("index"))


@app.route("/server/<int:server_id>")
def server_detail(server_id: int):
    server = get_server_by_id(server_id)
    if not server:
        abort(404)

    metrics = get_server_metrics(server_id)
    databases = get_databases_for_server(server_id)

    return render_template(
        "server.html",
        server=server,
        metrics=metrics,
        databases=databases,
    )


@app.route("/server/<int:server_id>/db/<path:db_name>")
def db_detail(server_id: int, db_name: str):
    # 1) Make sure server exists
    server = get_server_by_id(server_id)
    if not server:
        abort(404)

    # 2) Take a fresh snapshot for this DB into db_metadata
    snapshot_db_metadata(server_id, db_name)

    # 3) Load the latest metadata row
    db_meta = get_db_metadata(server_id, db_name)
    if not db_meta:
        abort(404)

    obj_summary = get_db_object_summary(db_name, server_name=server['name'])

    return render_template(
        "db.html",
        server=server,
        db=db_meta,
        obj_summary=obj_summary,
    )


@app.route(
    "/server/<int:server_id>/db/<path:db_name>/objects/download-multi",
    methods=["GET"]
)
def download_objects_multi(server_id: int, db_name: str):
    server = get_server_by_id(server_id)
    if not server:
        abort(404)

    raw_type = (request.args.get("type") or "table").lower()
    type_map = {
        "table": "TABLE", "tables": "TABLE",
        "view": "VIEW", "views": "VIEW",
        "procedure": "PROCEDURE", "procedures": "PROCEDURE", "proc": "PROCEDURE", "sp": "PROCEDURE",
        "function": "FUNCTION", "functions": "FUNCTION", "func": "FUNCTION",
    }
    obj_type = type_map.get(raw_type, "TABLE")

    selected_names = request.args.getlist("names") or request.args.getlist("name")
    if not selected_names:
        return make_response("No objects selected", 400)

    conn = get_sql_connection(server['name'], db_name)

    buf = io.StringIO()
    w = csv.writer(buf)

    for full_name in selected_names:
        meta = get_object_metadata(conn, db_name, full_name, obj_type)
        if not meta:
            continue

        e = meta.get("extras", {}) or {}
        referenced = meta.get("referenced", []) or []
        referencing = meta.get("referencing", []) or []
        ref_summary = meta.get("referenced_summary", {}) or {}

        # ----------------------------
        # 1) OVERVIEW (single neat row)
        # ----------------------------
        w.writerow(["OBJECT_OVERVIEW"])
        w.writerow([
            "full_name","type_desc","create_date","modify_date",
            "row_count","used_mb","reserved_mb","index_count",
            "is_partitioned","partition_count","partition_scheme","partition_function","partition_key",
            "parameter_count",
            "uses_temp_tables","uses_transactions","uses_cursor","uses_while_loop","uses_triggers"
            ])


        param_count = len(e.get("parameters", []) or [])

        w.writerow([
            meta.get("full_name"),
            meta.get("type_desc"),
            meta.get("create_date"),
            meta.get("modify_date"),
            
            e.get("row_count"),
            e.get("used_mb"),
            e.get("reserved_mb"),
            e.get("index_count"),

            e.get("is_partitioned") if obj_type == "TABLE" else "",
            e.get("partition_count") if obj_type == "TABLE" else "",
            e.get("partition_scheme") if obj_type == "TABLE" else "",
            e.get("partition_function") if obj_type == "TABLE" else "",
            e.get("partition_key") if obj_type == "TABLE" else "",

            (len(e.get("parameters", []) or []) if obj_type in ("PROCEDURE","FUNCTION") else ""),

            e.get("uses_temp_tables") if obj_type in ("PROCEDURE","VIEW") else "",
            e.get("uses_transactions") if obj_type in ("PROCEDURE",) else "",
            e.get("uses_cursor") if obj_type in ("PROCEDURE","VIEW") else "",
            e.get("uses_while_loop") if obj_type in ("PROCEDURE","VIEW") else "",
            e.get("uses_triggers") if obj_type in ("PROCEDURE","VIEW") else "",])


        # ----------------------------
        # 2) REFERENCED SUMMARY
        # ----------------------------
        

        # ----------------------------
        # 3) DEPENDENCIES (neat table)
        # ----------------------------
        w.writerow(["DEPENDENCIES"])
        w.writerow(["direction", "object_full_name", "type_desc"])

        for r in referenced:
            w.writerow(["ReferencedByThis", r.get("full_name"), r.get("type_desc")])

        for r in referencing:
            w.writerow(["ReferencesThis", r.get("full_name"), r.get("type_desc")])

        w.writerow([])

        # ----------------------------
        # 4) PARAMETERS (only for SP/FN)
        # ----------------------------
        params = e.get("parameters", []) or []
        if obj_type in ("PROCEDURE", "FUNCTION") and params:
            w.writerow(["PARAMETERS"])
            if obj_type == "PROCEDURE":
                w.writerow(["param_name","data_type","is_output","is_nullable","max_length","precision","scale"])
                for p in params:
                    w.writerow([
                        p.get("name"),
                        p.get("system_type_name") or p.get("data_type"),
                        p.get("is_output"),
                        p.get("is_nullable"),
                        p.get("max_length"),
                        p.get("precision"),
                        p.get("scale"),
                    ])
            else:
                w.writerow(["param_name","data_type","max_length","precision","scale"])
                for p in params:
                    w.writerow([
                        p.get("name"),
                        p.get("system_type_name") or p.get("data_type"),
                        p.get("max_length"),
                        p.get("precision"),
                        p.get("scale"),
                    ])
            w.writerow([])

        # ----------------------------
        # 5) TABLE-ONLY: PK / INDEXES / FKs
        # ----------------------------
        if obj_type == "TABLE":
            pk = e.get("primary_key") or {}
            if pk:
                w.writerow(["PRIMARY_KEY"])
                w.writerow(["pk_name", "pk_columns"])
                w.writerow([pk.get("name"), ", ".join(pk.get("columns", []) or [])])
                w.writerow([])

            idxs = e.get("indexes", []) or []
            if idxs:
                w.writerow(["INDEXES"])
                w.writerow(["index_name","type_desc","is_unique","is_primary_key","key_columns","included_columns","fragmentation_percent"])
                for idx in idxs:
                    w.writerow([
                        idx.get("name"),
                        idx.get("type_desc"),
                        idx.get("is_unique"),
                        idx.get("is_primary_key"),
                        ", ".join(idx.get("key_columns", []) or []),
                        ", ".join(idx.get("included_columns", []) or []),
                        idx.get("fragmentation_percent"),
                    ])
                w.writerow([])

            fks = e.get("foreign_keys", []) or []
            if fks:
                w.writerow(["FOREIGN_KEYS"])
                w.writerow(["fk_name","parent_table","parent_columns","ref_table","ref_columns"])
                for fk in fks:
                    w.writerow([
                        fk.get("name"),
                        fk.get("parent_table"),
                        ", ".join(fk.get("parent_columns", []) or []),
                        fk.get("ref_table"),
                        ", ".join(fk.get("ref_columns", []) or []),
                    ])
                w.writerow([])

            parts = e.get("partitions") or e.get("partition_info") or []
            if parts:
                w.writerow(["PARTITIONS"])
                w.writerow(["partition_number", "row_count", "reserved_mb", "used_mb", "data_compression"])
                for p in parts:
                    w.writerow([
                        p.get("partition_number"),
                        p.get("row_count"),
                        p.get("reserved_mb"),
                        p.get("used_mb"),
                        p.get("data_compression") or p.get("data_compression_desc"),
                        ])
                    w.writerow([])


        # Separator between objects
        w.writerow(["-----"])
        w.writerow([])

    conn.close()

    csv_data = buf.getvalue()
    buf.close()

    filename = f"{db_name}_{obj_type.lower()}_VISIBLE_selected.csv"
    bio = io.BytesIO(csv_data.encode("utf-8"))
    bio.seek(0)
    return send_file(
        bio,
        mimetype="text/csv; charset=utf-8",
        as_attachment=True,
        download_name=filename,
        max_age=0,
    )



@app.route("/server/<int:server_id>/db/<path:db_name>/objects")
def db_objects(server_id: int, db_name: str):
    server = get_server_by_id(server_id)
    if not server:
        abort(404)

    # Ensure DB metadata row exists / is fresh (header info)
    snapshot_db_metadata(server_id, db_name)
    db_meta = get_db_metadata(server_id, db_name)

    # Object inventory summary + lists
    objects_summary, objects_by_type = get_db_objects_summary_and_lists(server['name'], db_name)

    # --- read filters from query string (controls dropdown + list) ---
    raw_type = (request.args.get("type") or "table").lower()
    type_map = {
        "table": "TABLE",
        "tables": "TABLE",
        "view": "VIEW",
        "views": "VIEW",
        "sp": "PROCEDURE",
        "proc": "PROCEDURE",
        "procedure": "PROCEDURE",
        "function": "FUNCTION",
        "func": "FUNCTION",
    }

    # This is what template expects (e.g. "TABLE", "VIEW") based on query
    selected_type = request.args.get("type", "table").upper()
    # Canonical type for metadata queries
    obj_type = type_map.get(raw_type, "TABLE")

    # All selected object full names (schema.name) from the multi-select form
    selected_names = request.args.getlist("names")

    # Dependency trail (for clickable flow like: SP_abc > table1 > view1)
    # Format: "TYPE::schema.object|TYPE::schema.object|..."
    trail_param = (request.args.get("trail") or "").strip()

    def _parse_trail(raw: str):
        items = []
        if not raw:
            return items
        for part in raw.split("|"):
            part = (part or "").strip()
            if not part:
                continue
            if "::" not in part:
                continue
            t, n = part.split("::", 1)
            t = (t or "").upper().strip()
            n = (n or "").strip()
            if not t or not n:
                continue
            items.append({"type": t, "name": n})
        return items

    trail_items = _parse_trail(trail_param)

    # Single connection to the same instance as inventory DB
    conn = get_sql_connection(server['name'], db_name)

    selected_objects = []
    for full_name in selected_names:
        meta = get_object_metadata(
            conn,
            db_name,
            full_name,
            obj_type,
        )

        # Used by the UI to disable the "Download plan" button when no cached plan exists.
        if obj_type == "PROCEDURE":
            extras = meta.get("extras") or {}
            extras["has_plan"] = has_cached_proc_plan(conn, db_name, full_name)
            meta["extras"] = extras

        selected_objects.append({
            "name": full_name,
            "type": selected_type,  # keep UI behavior same as before
            "meta": meta,
        })

    conn.close()

    # If user is viewing exactly one object and there's no trail yet,
    # initialize the trail with the current selection (nice UX for "start" node).
    if not trail_items and len(selected_objects) == 1:
        only = selected_objects[0]
        if only.get("name") and only.get("type"):
            trail_items = [{"type": str(only["type"]).upper(), "name": only["name"]}]
            trail_param = f"{trail_items[0]['type']}::{trail_items[0]['name']}"

    # Build clickable breadcrumb links for the trail
    # Each step links back to that object, carrying trail up to that step.
    trail_links = []
    if trail_items:
        acc_parts = []
        for item in trail_items:
            acc_parts.append(f"{item['type']}::{item['name']}")
            trail_links.append(
                {
                    "type": item["type"],
                    "name": item["name"],
                    "type_q": item["type"].lower(),
                    "trail": "|".join(acc_parts),
                }
            )

    return render_template(
        "objects.html",
        server=server,
        db=db_meta,
        obj_summary=objects_summary,
        obj_lists=objects_by_type,
        selected_type=selected_type,
        selected_names=selected_names,
        selected_objects=selected_objects,
        trail_param=trail_param,
        trail_links=trail_links,
    )


@app.route("/server/<int:server_id>/db/<path:db_name>/objects/download")
def download_object(server_id: int, db_name: str):
    server = get_server_by_id(server_id)
    if not server:
        abort(404)

    raw_type = (request.args.get("type") or "table").lower()
    type_map = {
        "table": "TABLE",
        "tables": "TABLE",
        "view": "VIEW",
        "views": "VIEW",
        "procedure": "PROCEDURE",
        "procedures": "PROCEDURE",
        "proc": "PROCEDURE",
        "sp": "PROCEDURE",
        "function": "FUNCTION",
        "functions": "FUNCTION",
        "func": "FUNCTION",
    }
    selected_type = type_map.get(raw_type, "TABLE")
    selected_name = request.args.get("name")

    if not selected_name:
        # nothing selected, just go back to dashboard
        return redirect(url_for("db_objects", server_id=server_id, db_name=db_name))

    # DB connection for metadata
    conn = get_sql_connection(server['name'], db_name)

    # New signature: (conn, database_name, full_name, obj_type)
    object_meta = get_object_metadata(conn, db_name, selected_name, selected_type)
    conn.close()

    if not object_meta:
        abort(404)

    extras = object_meta.get("extras", {}) or {}
    referenced = object_meta.get("referenced", []) or []
    referencing = object_meta.get("referencing", []) or []
    ref_summary = object_meta.get("referenced_summary", {}) or {}

    # Build a CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)

    # ---------- 1) Overview section (applies to all object types) ----------
    writer.writerow(["SECTION", "PROPERTY", "VALUE"])
    writer.writerow(["Overview", "FullName", object_meta.get("full_name")])
    writer.writerow(["Overview", "Schema", object_meta.get("schema_name")])
    writer.writerow(["Overview", "Name", object_meta.get("name")])
    writer.writerow(["Overview", "TypeCode", object_meta.get("type_code")])
    writer.writerow(["Overview", "TypeDesc", object_meta.get("type_desc")])
    writer.writerow(["Overview", "Created", object_meta.get("create_date")])
    writer.writerow(["Overview", "LastModified", object_meta.get("modify_date")])

    # ---------- 1a) Table metrics in overview ----------
    if selected_type == "TABLE":
        writer.writerow(["Overview", "RowCount", extras.get("row_count")])
        writer.writerow(["Overview", "UsedMB", extras.get("used_mb")])
        writer.writerow(["Overview", "ReservedMB", extras.get("reserved_mb")])
        writer.writerow(["Overview", "IndexCount", extras.get("index_count")])

    # ---------- 1b) Procedure overview extras ----------
    if selected_type == "PROCEDURE":
        writer.writerow(["Overview", "ParameterCount", len(extras.get("parameters", []))])
        writer.writerow(["Overview", "UsesTempTables", extras.get("uses_temp_tables")])
        writer.writerow(["Overview", "UsesTransactions", extras.get("uses_transactions")])
        writer.writerow(["Overview", "UsesTryCatch", extras.get("uses_try_catch")])
        writer.writerow(["Overview", "UsesLinkedServer", extras.get("uses_linked_server")])
        writer.writerow(["Overview", "UsesCursor", extras.get("uses_cursor")])
        writer.writerow(["Overview", "UsesWhileLoop", extras.get("uses_while_loop")])
        writer.writerow(["Overview", "UsesTriggers", extras.get("uses_triggers")])

    # ---------- 1c) View overview extras ----------
    if selected_type == "VIEW":
        writer.writerow(["Overview", "IndexCount", extras.get("index_count")])
        writer.writerow(["Overview", "IsIndexedView", extras.get("is_indexed_view")])
        writer.writerow(["Overview", "IsSchemaBound", extras.get("is_schema_bound")])
        writer.writerow(["Overview", "UsesDistinct", extras.get("uses_distinct")])
        writer.writerow(["Overview", "UsesGroupBy", extras.get("uses_group_by")])
        writer.writerow(["Overview", "UsesUnion", extras.get("uses_union")])
        writer.writerow(["Overview", "UsesWindowFunctions", extras.get("uses_window_functions")])
        writer.writerow(["Overview", "UsesCursor", extras.get("uses_cursor")])
        writer.writerow(["Overview", "UsesWhileLoop", extras.get("uses_while_loop")])
        writer.writerow(["Overview", "UsesTriggers", extras.get("uses_triggers")])

    # ---------- 1d) Function overview extras ----------
    if selected_type == "FUNCTION":
        writer.writerow(["Overview", "FunctionType", extras.get("function_type")])
        writer.writerow(["Overview", "ReturnDataType", extras.get("return_data_type")])
        writer.writerow(["Overview", "IsInlineTVF", extras.get("is_inline_tvf")])
        writer.writerow(["Overview", "IsNonDeterministic", extras.get("is_nondeterministic")])

    # ---------- 1e) Referenced object summary ----------
    writer.writerow([])
    writer.writerow(["SECTION", "OBJECT_KIND", "COUNT"])
    writer.writerow(["ReferencedSummary", "TABLE", ref_summary.get("TABLE")])
    writer.writerow(["ReferencedSummary", "VIEW", ref_summary.get("VIEW")])
    writer.writerow(["ReferencedSummary", "PROCEDURE", ref_summary.get("PROCEDURE")])
    writer.writerow(["ReferencedSummary", "FUNCTION", ref_summary.get("FUNCTION")])
    writer.writerow(["ReferencedSummary", "OTHER", ref_summary.get("OTHER")])

    writer.writerow([])

    # ---------- 2) Procedure parameters ----------
    if selected_type == "PROCEDURE" and extras.get("parameters"):
        writer.writerow([
            "SECTION",
            "ParamName",
            "DataType",
            "IsOutput",
            "IsNullable",
            "MaxLength",
            "Precision",
            "Scale",
        ])
        for p in extras.get("parameters", []):
            writer.writerow([
                "ProcedureParameter",
                p.get("name"),
                p.get("system_type_name") or p.get("data_type"),
                p.get("is_output"),
                p.get("is_nullable"),
                p.get("max_length"),
                p.get("precision"),
                p.get("scale"),
            ])
        writer.writerow([])

    # ---------- 3) Function parameters ----------
    if selected_type == "FUNCTION" and extras.get("parameters"):
        writer.writerow([
            "SECTION",
            "ParamName",
            "DataType",
            "MaxLength",
            "Precision",
            "Scale",
        ])
        for p in extras.get("parameters", []):
            writer.writerow([
                "FunctionParameter",
                p.get("name"),
                p.get("system_type_name") or p.get("data_type"),
                p.get("max_length"),
                p.get("precision"),
                p.get("scale"),
            ])
        writer.writerow([])

    # ---------- 4) Table: primary key ----------
    if selected_type == "TABLE" and extras.get("primary_key"):
        pk = extras.get("primary_key") or {}
        writer.writerow(["SECTION", "PKName", "PKColumns"])
        writer.writerow([
            "PrimaryKey",
            pk.get("name"),
            ", ".join(pk.get("columns", [])),
        ])
        writer.writerow([])

    # ---------- 5) Table: indexes ----------
    if selected_type == "TABLE" and extras.get("indexes"):
        writer.writerow(
            [
                "SECTION",
                "IndexName",
                "IsPrimaryKey",
                "IsUnique",
                "Type",
                "KeyColumns",
                "IncludedColumns",
                "FragmentationPercent",
            ]
        )
        for idx in extras.get("indexes", []):
            writer.writerow(
                [
                    "Index",
                    idx.get("name"),
                    "Yes" if idx.get("is_primary_key") else "No",
                    "Yes" if idx.get("is_unique") else "No",
                    idx.get("type_desc"),
                    ", ".join(idx.get("key_columns", [])),
                    ", ".join(idx.get("included_columns", [])),
                    idx.get("fragmentation_percent"),
                ]
            )
        writer.writerow([])

    # ---------- 6) Table: foreign keys ----------
    if selected_type == "TABLE" and extras.get("foreign_keys"):
        writer.writerow(
            [
                "SECTION",
                "FKName",
                "ParentTable",
                "ParentColumns",
                "ReferencedTable",
                "ReferencedColumns",
            ]
        )
        for fk in extras.get("foreign_keys", []):
            writer.writerow(
                [
                    "ForeignKey",
                    fk.get("name"),
                    fk.get("parent_table"),
                    ", ".join(fk.get("parent_columns", [])),
                    fk.get("ref_table"),
                    ", ".join(fk.get("ref_columns", [])),
                ]
            )
        writer.writerow([])

    # ---------- 7) Dependencies: objects referenced BY this ----------
    writer.writerow(["SECTION", "Direction", "ObjectFullName", "TypeDesc"])
    for r in referenced:
        writer.writerow(
            [
                "Dependency",
                "ReferencedByThis",
                r.get("full_name"),
                r.get("type_desc"),
            ]
        )

    # ---------- 8) Dependencies: objects that REFERENCE this ----------
    for r in referencing:
        writer.writerow(
            [
                "Dependency",
                "ReferencesThis",
                r.get("full_name"),
                r.get("type_desc"),
            ]
        )

    csv_data = output.getvalue()
    output.close()

    safe_name = selected_name.replace(".", "_").replace(" ", "_")
    filename = f"{db_name}_{selected_type.lower()}_{safe_name}.csv"

    resp = Response(csv_data, mimetype="text/csv")
    resp.headers["Content-Disposition"] = f"attachment; filename={filename}"
    return resp


@app.route("/server/<int:server_id>/db/<path:db_name>/objects/sqlplan")
def download_sqlplan(server_id: int, db_name: str):
    """Download the *cached* execution plan for a stored procedure as .sqlplan.

    Notes:
    - This uses sys.dm_exec_procedure_stats + sys.dm_exec_query_plan.
    - If the procedure hasn't executed since the last restart / cache clear, there may be no row.
    - The browser decides where the file lands (typically the user's Downloads folder).
    """

    server = get_server_by_id(server_id)
    if not server:
        abort(404)

    sp_full_name = (request.args.get("name") or "").strip()
    if not sp_full_name:
        abort(400)

    conn = get_sql_connection(server['name'], db_name)
    try:
        cur = conn.cursor()

        # Make sure we're in the correct database context for OBJECT_ID
        cur.execute(f"USE [{db_name.replace(']', ']]')}]")

        sql = """
        SELECT TOP (1)
            qp.query_plan AS query_plan
        FROM sys.dm_exec_procedure_stats AS ps
        CROSS APPLY sys.dm_exec_query_plan(ps.plan_handle) AS qp
        WHERE ps.object_id = OBJECT_ID(?)
        ORDER BY ps.last_execution_time DESC;
        """
        cur.execute(sql, sp_full_name)
        row = cur.fetchone()
        print(row)
    finally:
        conn.close()

    if not row or not row[0]:
        # No cached plan available (SP not executed / cache cleared). Keep API honest.
        msg = (
            f"No cached execution plan found for {sp_full_name} in {db_name}. "
            "Run the stored procedure once (or ensure it is in cache), then try again."
        )
        return make_response(msg, 404)

    xml_plan = str(row[0])
    bio = io.BytesIO(xml_plan.encode("utf-8"))
    bio.seek(0)

    safe = sp_full_name.replace("[", "").replace("]", "").replace(".", "_").replace(" ", "_")
    filename = f"{safe}.sqlplan"

    return send_file(
        bio,
        mimetype="application/xml",
        as_attachment=True,
        download_name=filename,
        max_age=0,
    )




# --------------------------------------------------
# DataSolveX Integration Hook
# --------------------------------------------------
def get_inventory_app(shared_secret_key: str | None = None):
    """Return the inventory Flask app for mounting under /inventory-mgmt.

    - Uses the same secret key as the main DataSolveX app so the `session`
      cookie can be read consistently.
    - Protects inventory routes behind the DataSolveX login session.
    """
    if shared_secret_key:
        app.secret_key = shared_secret_key

    # Add a login-gate once (idempotent)
    if not getattr(app, "_datasolvex_login_gate", False):
        from flask import session as _session

        @app.before_request
        def _require_login():
            # Allow static assets.
            # When mounted under /inventory-mgmt, Flask may see paths like
            #   /inventory-mgmt/static/... or /static/...
            # depending on how URLs were built and how SCRIPT_NAME is applied.
            if request.endpoint == "static":
                return None
            if "/static/" in request.path:
                return None
            # Only allow if DataSolveX session exists
            if _session.get("login_name"):
                return None
            return redirect("/login")

        app._datasolvex_login_gate = True

    return app
