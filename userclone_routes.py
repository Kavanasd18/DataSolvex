import os
import subprocess
import time
import json
from datetime import datetime
 
import pyodbc
from flask import Blueprint, render_template, request, jsonify, Response, session, redirect, url_for
from dotenv import load_dotenv 
# -----------------------------------------------------------------------------
# User Clone Management Tool (Blueprint)
# Integrated into DataSolveX main Flask app.
# URL prefix recommended: /userclone
# -----------------------------------------------------------------------------
 
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))




userclone_bp = Blueprint("userclone", __name__)
 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RUN_DIR = os.path.join(BASE_DIR, "runs")
os.makedirs(RUN_DIR, exist_ok=True)
 
POWERSHELL_SCRIPT = os.path.join(RUN_DIR, "clone_script.ps1")
LOG_CONFIG_FILE = os.path.join(RUN_DIR, "logging_config.txt")
POWERSHELL_EXE = "powershell.exe"
 
# DEFAULT_LOGGING_SERVER = "ISMGMTDBP02\INST1"
# DEFAULT_LOGGING_DB = "DBRefresh"


LAST_BULK_PATH_FILE = os.path.join(RUN_DIR, "filepath.txt")



def save_last_bulk_path(path: str):
    path = (path or "").strip()
    if not path:
        return
    os.makedirs(os.path.dirname(LAST_BULK_PATH_FILE), exist_ok=True)
    with open(LAST_BULK_PATH_FILE, "w", encoding="utf-8") as f:
        f.write(path)

def load_last_bulk_path() -> str:
    try:
        if not os.path.exists(LAST_BULK_PATH_FILE):
            return ""
        with open(LAST_BULK_PATH_FILE, "r", encoding="utf-8") as f:
            return (f.read() or "").strip()
    except Exception:
        return ""

# ---------------- Logging Config (for execution log DB) ----------------
def get_logging_config():
    
 
    with open(LOG_CONFIG_FILE, "r") as f:
        lines = f.readlines()
 
    server = lines[0].split(":", 1)[1].strip()
    database = lines[1].split(":", 1)[1].strip()
 
    return {"server": server, "database": database}
 
 
def write_logging_config(server, database):
    with open(LOG_CONFIG_FILE, "w") as f:
        f.write(f"Logging Server: {server}\n")
        f.write(f"Logging Database: {database}\n")
 
 
def pick_pyodbc_driver():
    drivers = pyodbc.drivers()
    preferred = [
        "ODBC Driver 18 for SQL Server",
        "ODBC Driver 17 for SQL Server",
        "ODBC Driver 13 for SQL Server",
        "SQL Server Native Client 11.0",
    ]
    for d in preferred:
        if d in drivers:
            return d
    return drivers[-1]
 
 
def sql_connect_and_list(server, timeout=5):
    """
    Try connecting to the given SQL Server and list online databases.
    Always returns: (success: bool, message: str, databases: list[str])
    """
    driver = pick_pyodbc_driver()
    try:
        conn = pyodbc.connect(
            f"DRIVER={{{driver}}};"
            f"SERVER={server};"
            f"DATABASE=master;"
            f"Trusted_Connection=yes;"
            f"Encrypt=no;"
            f"TrustServerCertificate=yes;"
            f"Connection Timeout={timeout};",
            autocommit=True,
        )

        cur = conn.cursor()
        cur.execute("SELECT name FROM sys.databases WHERE state = 0 ORDER BY name")
        dbs = [r[0] for r in cur.fetchall()]
        conn.close()

        return True, "Connection successful", dbs

    except pyodbc.Error as e:
        # Keep message short + user-friendly
        return False, f"Invalid server or connection failed: {server}", []
    except Exception:
        return False, f"Invalid server or connection failed: {server}", []

 
 
def get_logging_connection(server, database):
    driver = pick_pyodbc_driver()
    return pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
        f"Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;",
        autocommit=True,
    )
 
def get_execution_log_connection():
    driver = pick_pyodbc_driver()
    uc_logServer = os.getenv("uc_logServer")
    uc_logDatabase= os.getenv("uc_logDatabase")
    return pyodbc.connect(
        f"DRIVER={{{driver}}};"
        f"SERVER={uc_logServer};"
        f"DATABASE={uc_logDatabase};"
        f"Trusted_Connection=yes;"
        f"Encrypt=no;"
        f"TrustServerCertificate=yes;",
        autocommit=True,
    )
 
# ---------------- SSE ----------------
def sse(event, data):
    safe = str(data).replace("\n", "\\n")
    return f"event: {event}\ndata: {safe}\n\n"
 
 
def _require_login():
    """Simple gate: user must be logged in to use this tool."""
    return "login_name" in session
 
 
# ---------------- UI ----------------
@userclone_bp.route("/")
def userclone_home():
    if not _require_login():
        return redirect(url_for("login"))
    return render_template("userclone.html")
 
 
# ---------------- API: test connection + list DBs ----------------
@userclone_bp.route("/test_connection", methods=["POST"])
def test_connection():
    if not _require_login():
        return jsonify(success=False, message="Not logged in.", databases=[]), 401

    server = request.form.get("server", "").strip()
    if not server:
        return jsonify(success=False, message="Source server is required.", databases=[]), 400

    ok, msg, dbs = sql_connect_and_list(server)
    return jsonify(success=ok, message=msg, databases=dbs if ok else [])

 
 
@userclone_bp.route("/get_databases", methods=["POST"])
def get_databases():
    if not _require_login():
        return jsonify(success=False, message="Not logged in.", databases=[]), 401

    server = request.form.get("server", "").strip()
    if not server:
        return jsonify(success=False, message="Source server is required.", databases=[]), 400

    ok, msg, dbs = sql_connect_and_list(server)
    return jsonify(success=ok, message=msg, databases=dbs if ok else [])

 
 
# ---------------- API: run PowerShell script (stream output) ----------------
@userclone_bp.route("/run_script_stream", methods=["POST"])
def run_script_stream():
    form = request.form
 
    server = form.get("server", "").strip()
    database = form.get("database", "").strip()
 
    logging_cfg = get_logging_config()
    logging_server = logging_cfg["server"]
    logging_database = logging_cfg["database"]
 
    mode = form.get("mode", "").strip()
    executed_by = session.get("login_name")
    whoCreated = session.get("login_name")
 
    if not executed_by:
        return Response(
            sse("error", "Session expired. Please login again."),
            mimetype="text/event-stream"
        )
    user_pair = form.get("user_pair", "").strip()
 
    if not server or not database:
        return Response(sse("output", "Server and database required"), mimetype="text/event-stream")
 
    # ---------------- MULTIPLE MODE: TEXT PATH ONLY ----------------
    user_file_path = None
    if mode == "Multiple":
        user_file_path = form.get("user_file_path", "").strip()
        if not user_file_path:
            return Response(sse("output", "File path required"), mimetype="text/event-stream")
        
        save_last_bulk_path(user_file_path)
 
    # ---------------- SINGLE MODE VALIDATION ----------------
    if mode == "Single" and "," not in user_pair:
        return Response(sse("output", "Invalid user pair"), mimetype="text/event-stream")
 
    # Insert log
    cn = get_execution_log_connection()
 
    cur = cn.cursor()
 
    exec_mode = "Bulk" if mode == "Multiple" else mode
 
    cur.execute(
        """
        INSERT INTO dbo.ExecutionLog (ExecutedBy, StartTime, Mode, Status)
        VALUES (?, GETDATE(), ?, 'Running')
        """,
        (executed_by, exec_mode),
    )
 
    cn.close()
 
    # Build PowerShell command
    cmd = [
        POWERSHELL_EXE,
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", POWERSHELL_SCRIPT,
        "-ServerName", server,
        "-DBName", database,
        "-LoggingServer", logging_server,
        "-LoggingDB", logging_database,
        "-Mode", mode,
        "-ExecutedBy", executed_by,
    ]
 
    if mode == "Single":
        cmd += ["-UserPair", user_pair]
    else:
        cmd += ["-UserFile", user_file_path]
    ## new
    error_buffer = []
    def generate():
        start = time.time()
        yield sse("output", "Running PowerShell")
 
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1
        )
 
        for raw in iter(proc.stdout.readline, ''):
            line = raw.rstrip()
 
            # ---- PROGRESS HANDLING ----
            if line.startswith("__PROGRESS__"):
                try:
                    _, pct, stage = line.split(":", 2)
                    yield sse(
                        "progress",
                         json.dumps({"percent": int(pct)})
                    )
                except:
                    pass
                continue
 
            # ---- ERROR HANDLING ----
            if line.startswith("__ERROR__"):
                msg = line.replace("__ERROR__", "").strip()
                error_buffer.append(msg)
                yield sse("error", msg)
                continue
            error_flag = (
                line.startswith("ERROR:")
                or line.startswith("Error:")
                or "Exception calling" in line
                or "CategoryInfo" in line
                or "FullyQualifiedErrorId" in line
            )
            if error_flag:
                error_buffer.append(line)
                yield sse("error", line)
            else:
                yield sse("output", line)
 
        proc.wait()
        rc = proc.returncode
 
        duration = int(time.time() - start)
        status = "Success" if rc == 0 else "Failed"
 
        error_message = None
        if status == "Failed":
            error_message = (
                "\n".join(error_buffer[:4000])
                if error_buffer
                else "PowerShell execution failed with no captured output"
            )
 
        cn2 = get_execution_log_connection()
        cur2 = cn2.cursor()
        cur2.execute(
            """
            UPDATE dbo.ExecutionLog
            SET EndTime = GETDATE(),
                DurationSeconds = ?,
                Status = ?,
                ErrorMessage = ?
            WHERE ExecutedBy = ?
              AND Mode = ?
              AND Status = 'Running'
              AND EndTime IS NULL
            """,
            (duration, status, error_message, executed_by, exec_mode),
        )
        cn2.close()
 
        yield sse("done", json.dumps({"duration": duration, "status": status}))
    return Response(generate(), mimetype="text/event-stream")

@userclone_bp.route("/last_saved_bulk_path", methods=["GET"])
def last_saved_bulk_path():
    if not _require_login():
        return jsonify(success=False, path=""), 401

    path = load_last_bulk_path()
    return jsonify(success=True, path=path)

@userclone_bp.route("/last_saved_pairs", methods=["GET"])
def last_saved_pairs():
    """
    Reads runs/filepath.txt -> opens that .txt -> parses old,new per line -> returns pairs.
    """
    if not _require_login():
        return jsonify(success=False, message="Not logged in", pairs=[]), 401

    path = load_last_bulk_path()
    if not path:
        return jsonify(success=True, message="No saved bulk txt path found", pairs=[])

    if not os.path.exists(path):
        return jsonify(success=True, message=f"Saved txt not found: {path}", pairs=[])

    pairs = []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.strip()
                if not line or "," not in line:
                    continue
                old_u, new_u = [p.strip() for p in line.split(",", 1)]
                if old_u and new_u:
                    pairs.append({"old_user": old_u, "new_user": new_u})

        return jsonify(success=True, message="", source_path=path, pairs=pairs)

    except Exception as e:
        return jsonify(success=True, message="Failed to read saved txt", pairs=[])

 
@userclone_bp.route("/read_pairs_from_path", methods=["POST"])
def read_pairs_from_path():
    """
    Reads a txt path sent from UI -> parses old,new per line -> returns pairs.
    Also saves the path as last_bulk_path (runs/filepath.txt) so Validate remembers it.
    """
    if not _require_login():
        return jsonify(success=False, message="Not logged in", pairs=[]), 401

    path = (request.form.get("path") or "").strip()
    if not path:
        return jsonify(success=False, message="Path required", pairs=[])

    if not path.lower().endswith(".txt"):
        return jsonify(success=False, message="Only .txt files are supported", pairs=[])

    if not os.path.exists(path):
        return jsonify(success=False, message=f"File not found: {path}", pairs=[])

    pairs = []
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for raw in f:
                line = raw.strip()
                if not line or "," not in line:
                    continue
                old_u, new_u = [p.strip() for p in line.split(",", 1)]
                if old_u and new_u:
                    pairs.append({"old_user": old_u, "new_user": new_u})

        # ✅ Save so next time Validate opens it will auto-load
        save_last_bulk_path(path)

        return jsonify(success=True, saved_path=path, pairs=pairs)

    except Exception as e:
        return jsonify(success=False, message="Failed to read txt file", pairs=[])


# ---------------- Logs endpoints ----------------
@userclone_bp.route("/recent_5", methods=["GET"])
def recent_5():
    log_server = request.args.get("log_server", "")
    log_db = request.args.get("log_db", "")
 
    cn = get_execution_log_connection()
 
    cur = cn.cursor()
    cur.execute(
        """
        SELECT TOP 5 ExecutedBy, StartTime, EndTime, DurationSeconds, Mode, Status,ErrorMessage
        FROM dbo.ExecutionLog
        ORDER BY StartTime DESC
        """
    )
    rows = cur.fetchall()
    cn.close()
 
    return jsonify([
        dict(
            executed_by=r[0],
            start_time=r[1].strftime("%Y-%m-%d %H:%M:%S"),
            end_time=r[2].strftime("%Y-%m-%d %H:%M:%S") if r[2] else "",
            duration_seconds=r[3],
            mode=r[4],
            status=r[5],
            error_message=r[6]
        )
        for r in rows
    ])
 
 
@userclone_bp.route("/all_logs", methods=["GET"])
def all_logs():
    log_server = request.args.get("log_server", "")
    log_db = request.args.get("log_db", "")
 
    cn = get_execution_log_connection()
    ##new
    cur = cn.cursor()
    cur.execute(
        """
        SELECT ExecutedBy, StartTime, EndTime, DurationSeconds, Mode, Status,ErrorMessage
        FROM dbo.ExecutionLog
        ORDER BY StartTime DESC
        """
    )
    rows = cur.fetchall()
    cn.close()
 
    return jsonify([
        dict(
            executed_by=r[0],
            start_time=r[1].strftime("%Y-%m-%d %H:%M:%S"),
            end_time=r[2].strftime("%Y-%m-%d %H:%M:%S") if r[2] else "",
            duration_seconds=r[3],
            mode=r[4],
            status=r[5],
            error_message=r[6]
        )
        for r in rows
    ])
 
 
@userclone_bp.route("/save_logging_settings", methods=["POST"])
def save_logging_settings():
    if not _require_login():
        return jsonify(success=False, message="Not logged in."), 401
 
    server = request.form.get("logging_server", "").strip()
    database = request.form.get("logging_database", "").strip()
    write_logging_config(server, database)
    return jsonify(success=True)
 
 
@userclone_bp.route("/logging_defaults", methods=["GET"])
def logging_defaults():
    if not _require_login():
        return jsonify(success=False, message="Not logged in."), 401
    return jsonify(success=True, **get_logging_config())
 


@userclone_bp.route("/validation_logs", methods=["GET"])
def validation_logs():
    """
    Returns CloneValidation rows from Logging DB.
    UI calls: /userclone/validation_logs?log_server=...&log_db=...
    """
    try:
        log_server = request.args.get("log_server", "").strip()
        log_db = request.args.get("log_db", "").strip()

        if not log_server or not log_db:
            return jsonify([])

        conn_str = (
            "DRIVER={ODBC Driver 17 for SQL Server};"
            f"SERVER={log_server};"
            f"DATABASE={log_db};"
            "Trusted_Connection=yes;"
        )

        conn = pyodbc.connect(conn_str, timeout=10)
        cur = conn.cursor()

        # table might not exist yet if no clone run happened
        cur.execute("""
            IF OBJECT_ID('dbo.CloneValidation','U') IS NULL
            BEGIN
                SELECT CAST(NULL AS NVARCHAR(100)) AS OldUser,
                       CAST(NULL AS NVARCHAR(100)) AS NewUser,
                       CAST(NULL AS NVARCHAR(20))  AS ValidationStatus,
                       CAST(NULL AS DATETIME)       AS [Timestamp],
                       CAST(NULL AS NVARCHAR(255))  AS Remarks
                WHERE 1=0;
            END
            ELSE
            BEGIN
                SELECT TOP (500)
                       OldUser,
                       NewUser,
                       ValidationStatus,
                       [Timestamp],
                       Remarks
                FROM dbo.CloneValidation
                ORDER BY [Timestamp] DESC;
            END
        """)

        rows = cur.fetchall()
        conn.close()

        out = []
        for r in rows:
            out.append({
                "old_user": r[0],
                "new_user": r[1],
                "status": r[2],
                "timestamp": r[3].strftime("%Y-%m-%d %H:%M:%S") if r[3] else "",
                "remarks": r[4] if len(r) > 4 else ""
            })

        return jsonify(out)

    except Exception as e:
        # keep UI stable: return empty list
        return jsonify([])
