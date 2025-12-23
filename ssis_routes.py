import os
import datetime

import pyodbc
from flask import Blueprint, render_template, request, jsonify, session


# ---------------------------------------------------------
# SSIS Package Connection Updates (Blueprint)
# Mounted under: /ssis/*
# ---------------------------------------------------------

ssis_bp = Blueprint("ssis", __name__, template_folder="templates")


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
SQL_SCRIPT_PATH = os.path.join(BASE_DIR, "scripts", "ssis", "ssis_update.sql")


# ---------------------------------------------------------
# LOGGING SERVER + DATABASE (FIXED)
# ---------------------------------------------------------
LOG_SERVER = "ISMGMTDBP02\INST1"
LOG_DATABASE = "DBRefresh"


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
    raise RuntimeError("No suitable SQL Server ODBC driver found")


def conn_str(server, db=None):
    if not server:
        raise ValueError("Server name is required")

    driver = pick_pyodbc_driver()
    cs = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"Trusted_Connection=yes;"
        f"Encrypt=no;"
        f"TrustServerCertificate=yes;"
    )
    if db:
        cs += f"DATABASE={db};"
    return cs


def format_duration(value):
    if value is None:
        return "0.00"

    # SQL DECIMAL → Python float / Decimal
    return f"{float(value):.3f}"



def load_sql_script(config_table, backup_table):
    with open(SQL_SCRIPT_PATH, "r", encoding="utf-8") as f:
        sql = f.read()
    sql = sql.replace("__CONFIG_TABLE__", config_table)
    sql = sql.replace("__BACKUP_TABLE__", backup_table)
    return sql


def fetch_recent():
    conn = pyodbc.connect(conn_str(LOG_SERVER, LOG_DATABASE))
    cur = conn.cursor()
    cur.execute(
        """
        SELECT TOP 5 ExecutedBy, StartTime, EndTime, Duration, ExecutionStatus
        FROM ExecutionLogs
        ORDER BY StartTime DESC
        """
    )
    rows = cur.fetchall()
    conn.close()

    out = []
    for r in rows:
        out.append(
            {
                "ExecutedBy": r.ExecutedBy,
                "StartTime": r.StartTime.strftime("%Y-%m-%d %H:%M:%S"),
                "EndTime": r.EndTime.strftime("%Y-%m-%d %H:%M:%S") if r.EndTime else "",
                "Duration": format_duration(r.Duration),
                "ExecutionStatus": r.ExecutionStatus,
            }
        )
    return out


def fetch_all_history():
    conn = pyodbc.connect(conn_str(LOG_SERVER, LOG_DATABASE))
    cur = conn.cursor()
    cur.execute(
        """
        SELECT ExecutedBy, StartTime, EndTime, Duration, ExecutionStatus
        FROM ExecutionLogs
        ORDER BY StartTime DESC
        """
    )
    rows = cur.fetchall()
    conn.close()
    return rows


@ssis_bp.route("/")
def ssis_home():
    return render_template("ssis/index.html", recent=fetch_recent())


@ssis_bp.route("/all_logs_json")
def all_logs_json():
    rows = fetch_all_history()
    return jsonify(
        [
            {
                "executed_by": r.ExecutedBy,
                "start_time": r.StartTime.strftime("%Y-%m-%d %H:%M:%S"),
                "end_time": r.EndTime.strftime("%Y-%m-%d %H:%M:%S") if r.EndTime else "",
                "duration": format_duration(r.Duration),
                "status": r.ExecutionStatus,
            }
            for r in rows
        ]
    )


@ssis_bp.route("/get_databases", methods=["POST"])
def get_databases():
    data = request.get_json() or {}
    server = (data.get("server") or "").strip()

    if not server:
        return jsonify(success=False, message="Server name required")

    try:
        conn = pyodbc.connect(conn_str(server))
        cur = conn.cursor()
        cur.execute(
            """
            SELECT name
            FROM sys.databases
            WHERE state = 0
            ORDER BY name
            """
        )
        dbs = [r[0] for r in cur.fetchall()]
        conn.close()
        return jsonify(success=True, databases=dbs)
    except Exception as e:
        return jsonify(success=False, message=str(e))


@ssis_bp.route("/run_update_stream", methods=["POST"])
def run_update():
    server = (request.form.get("server") or "").strip()
    database = (request.form.get("database") or "").strip()
    config_table = (request.form.get("config_table") or "").strip()
    backup_table = (request.form.get("backup_table") or "").strip()

    executed_by = session.get("login_name")
    if not executed_by:
        return jsonify(status="Failed", error="Session expired. Please login again.")

    if not all([server, database, config_table, backup_table]):
        return jsonify(status="Failed", error="All fields are required")

    sql_script = load_sql_script(config_table, backup_table)
    start_time = datetime.datetime.now()

    try:
        conn = pyodbc.connect(conn_str(server, database))
        cur = conn.cursor()
        cur.execute(sql_script)
        conn.commit()
        conn.close()
        status = "Success"
        error_msg = None
    except Exception as e:
        status = "Failed"
        error_msg = str(e)

    end_time = datetime.datetime.now()
    duration = round((end_time - start_time).total_seconds(), 3)

    # Log execution (best-effort)
    try:
        log_conn = pyodbc.connect(conn_str(LOG_SERVER, LOG_DATABASE))
        log_cur = log_conn.cursor()
        log_cur.execute(
            """
            INSERT INTO ExecutionLogs
            (ExecutedBy, StartTime, EndTime,Duration, ExecutionStatus)
            VALUES (?, ?, ?, CAST(? AS DECIMAL(10,3)), ?)
            """,
            executed_by,
            start_time,
            end_time,
            duration,
            status,
        )
        log_conn.commit()
        log_conn.close()
    except:
        pass

    if status == "Success":
        return jsonify(status="Success")
    else:
        return jsonify(status="Failed", error=error_msg)
