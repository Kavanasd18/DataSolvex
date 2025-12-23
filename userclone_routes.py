import os
import subprocess
import time
import json
from datetime import datetime

import pyodbc
from flask import Blueprint, render_template, request, jsonify, Response, session, redirect, url_for

# -----------------------------------------------------------------------------
# User Clone Management Tool (Blueprint)
# Integrated into DataSolveX main Flask app.
# URL prefix recommended: /userclone
# -----------------------------------------------------------------------------

userclone_bp = Blueprint("userclone", __name__)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts", "userclone")

POWERSHELL_SCRIPT = os.path.join(SCRIPTS_DIR, "clone_script.ps1")
LOG_CONFIG_FILE = os.path.join(SCRIPTS_DIR, "logging_config.txt")
POWERSHELL_EXE = "powershell.exe"

# DEFAULT_LOGGING_SERVER = "ISMGMTDBP02\INST1"
# DEFAULT_LOGGING_DB = "DBRefresh"

# ---------------- Logging Config (for execution log DB) ----------------
def get_logging_config():
    if not os.path.exists(LOG_CONFIG_FILE):
        write_logging_config(DEFAULT_LOGGING_SERVER, DEFAULT_LOGGING_DB)

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
    driver = pick_pyodbc_driver()
    conn = pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE=master;"
        f"Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;"
        f"Connection Timeout={timeout};",
        autocommit=True,
    )
    cur = conn.cursor()
    cur.execute("SELECT name FROM sys.databases WHERE state = 0 ORDER BY name")
    dbs = [r[0] for r in cur.fetchall()]
    conn.close()
    return True, "Connection successful", dbs


def get_logging_connection(server, database):
    driver = pick_pyodbc_driver()
    return pyodbc.connect(
        f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
        f"Trusted_Connection=yes;Encrypt=no;TrustServerCertificate=yes;",
        autocommit=True,
    )

def get_execution_log_connection():
    driver = pick_pyodbc_driver()
    return pyodbc.connect(
        f"DRIVER={{{driver}}};"
        f"SERVER=ISMGMTDBP02\INST1;"
        f"DATABASE=DBRefresh;"
        f"Trusted_Connection=yes;"
        f"Encrypt=no;"
        f"TrustServerCertificate=yes;",
        autocommit=True,
    )

# ---------------- SSE ----------------
def sse(event, data):
    safe = data.replace("\n", "\\n")
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
        return jsonify(success=False, message="Not logged in."), 401

    server = request.form.get("server", "").strip()
    ok, msg, dbs = sql_connect_and_list(server)
    return jsonify(success=ok, message=msg, databases=dbs)


@userclone_bp.route("/get_databases", methods=["POST"])
def get_databases():
    if not _require_login():
        return jsonify(success=False, message="Not logged in."), 401

    server = request.form.get("server", "").strip()
    ok, msg, dbs = sql_connect_and_list(server)
    return jsonify(success=ok, message=msg, databases=dbs)


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
    ]

    if mode == "Single":
        cmd += ["-UserPair", user_pair]
    else:
        cmd += ["-UserFile", user_file_path]

    def generate():
        start = time.time()
        yield sse("output", "Running PowerShell")

        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)

        for raw in proc.stdout:
            line = raw.rstrip()

            if line.startswith("__ERROR__"):
                yield sse("error", line.replace("__ERROR__", "").strip())
                continue

            if line.startswith("__PROGRESS__:"):
                try:
                    _, pct, stage = line.split(":", 2)
                    yield sse("progress", json.dumps({"percent": int(pct), "stage": stage}))
                except:
                    yield sse("output", line)
                continue

            if line.startswith("__DOWNLOAD__"):
                try:
                    _, filename, b64 = line.split(":", 2)
                    yield sse("download", json.dumps({"filename": filename, "data": b64}))
                except Exception as e:
                    yield sse("error", f"Download marker error: {str(e)}")
                continue

            # Normal/error output
            error_flag = (
                line.startswith("Error:")
                or "Exception calling" in line
                or "CategoryInfo" in line
                or "FullyQualifiedErrorId" in line
                or line.strip().startswith("C:\\")
            )

            yield sse("error" if error_flag else "output", line)

        rc = proc.wait()
        duration = int(time.time() - start)
        status = "Success" if rc == 0 else "Failed"

        # Update log
        cn2 = get_execution_log_connection()

        cur2 = cn2.cursor()
        cur2.execute(
            """
            UPDATE dbo.ExecutionLog
            SET EndTime = GETDATE(),
                DurationSeconds = ?,
                Status = ?
            WHERE ExecutedBy = ?
              AND Mode = ?
              AND Status = 'Running'
              AND EndTime IS NULL
            """,
            (duration, status, executed_by,exec_mode),
        )
        cn2.close()

        yield sse("done", json.dumps({"duration": duration, "status": status}))

    return Response(generate(), mimetype="text/event-stream")


# ---------------- Logs endpoints ----------------
@userclone_bp.route("/recent_5", methods=["GET"])
def recent_5():
    log_server = request.args.get("log_server", "")
    log_db = request.args.get("log_db", "")

    cn = get_execution_log_connection()

    cur = cn.cursor()
    cur.execute(
        """
        SELECT TOP 5 ExecutedBy, StartTime, EndTime, DurationSeconds, Mode, Status
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
        )
        for r in rows
    ])


@userclone_bp.route("/all_logs", methods=["GET"])
def all_logs():
    log_server = request.args.get("log_server", "")
    log_db = request.args.get("log_db", "")

    cn = get_execution_log_connection()

    cur = cn.cursor()
    cur.execute(
        """
        SELECT ExecutedBy, StartTime, EndTime, DurationSeconds, Mode, Status
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
