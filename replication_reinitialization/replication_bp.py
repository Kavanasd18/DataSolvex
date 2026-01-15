# replication_reinit/replication_bp.py
 
from flask import Blueprint, render_template, request, Response, jsonify
import subprocess
import os
import re
import json
import time
from datetime import datetime
import pyodbc
from dotenv import load_dotenv
 
replication_bp = Blueprint(
    "replication",
    __name__,
    template_folder="templates",
    static_folder="static",
    url_prefix="/replication"
)
 
# -----------------------------
# PowerShell config
# -----------------------------
POWERSHELL_EXE = r"powershell.exe"
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
 
SCRIPT1_PATH = os.path.join(BASE_DIR, "scripts", "ReplicationDropAndScript.ps1")
SCRIPT2_PATH = os.path.join(BASE_DIR, "scripts", "ExecuteSqlFile.ps1")
 
RUN_DIR = os.path.join(BASE_DIR, "runs")
os.makedirs(RUN_DIR, exist_ok=True)
 
LAST_SCRIPT1_CONTEXT_PATH = os.path.join(RUN_DIR, "last_script1_context.json")
 
PATH_TXT = os.path.join(BASE_DIR, "path.txt")
PUBLICATION_TXT = os.path.join(BASE_DIR, "publication.txt")

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

DEFAULT_LOG_INSTANCE = os.getenv("DEFAULT_LOG_INSTANCE")
DEFAULT_LOG_DATABASE = os.getenv("DEFAULT_LOG_DATABASE")
REPL_LOG_TABLE = os.getenv("REPL_LOG_TABLE")
REINIT_LOG_TABLE = os.getenv("REINIT_LOG_TABLE")
 
 
# -----------------------------
# SQL helpers
# -----------------------------
def _pyodbc_conn(server: str, database: str):
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str, timeout=10)
 
 
def fetch_recent_reinit_log(log_instance: str = "", log_database: str = "", log_table: str = "", limit: int = 5):
    log_instance = (log_instance or DEFAULT_LOG_INSTANCE).strip()
    log_database = (log_database or DEFAULT_LOG_DATABASE).strip()
    log_table = (log_table or REINIT_LOG_TABLE).strip()
    if not log_instance or not log_database or not log_table:
        return []
 
    sql = f"""
SELECT TOP ({int(limit)})
    Reinitalize_instance AS reinit_instance,
    publication_name,
    executed_by,
    initiated_time,
    status,
    error_message
FROM dbo.[{log_table}]
ORDER BY initiated_time DESC;
"""
    out = []
    try:
        with _pyodbc_conn(log_instance, log_database) as cn:
            cur = cn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                rec = dict(zip(cols, row))
                if rec.get("initiated_time"):
                    rec["initiated_time"] = rec["initiated_time"].strftime("%Y-%m-%d %H:%M:%S")
                out.append(rec)
    except Exception:
        return []
    return out
 
 
def fetch_recent_repl_log(log_instance: str = "", log_database: str = "", log_table: str = "", limit: int = 5):
    log_instance = (log_instance or DEFAULT_LOG_INSTANCE).strip()
    log_database = (log_database or DEFAULT_LOG_DATABASE).strip()
    log_table = (log_table or REPL_LOG_TABLE).strip()
    if not log_instance or not log_database or not log_table:
        return []
 
    sql = f"""
SELECT TOP ({int(limit)})
    dropped_on,
    table_name,
    publication_dropped,
    instance_name,
    [user],
    reinitialized_instance_name,
    publication_reinitialized,
    status,
    error_message
FROM dbo.[{log_table}]
ORDER BY dropped_on DESC;
"""
    out = []
    try:
        with _pyodbc_conn(log_instance, log_database) as cn:
            cur = cn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            for row in cur.fetchall():
                rec = dict(zip(cols, row))
                if rec.get("dropped_on"):
                    rec["dropped_on"] = rec["dropped_on"].strftime("%Y-%m-%d %H:%M:%S")
                out.append(rec)
    except Exception:
        return []
    return out
 
 
def fetch_full_table(log_instance: str, log_database: str, table: str):
    if not log_instance or not log_database or not table:
        return [], []
 
    sql = f"SELECT * FROM dbo.[{table}] ORDER BY 1 DESC"
    try:
        with _pyodbc_conn(log_instance, log_database) as cn:
            cur = cn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = []
            for r in cur.fetchall():
                rr = []
                for v in r:
                    if hasattr(v, "strftime"):
                        rr.append(v.strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        rr.append(v)
                rows.append(rr)
            return cols, rows
    except Exception:
        return [], []
 
 
# -----------------------------
# TXT persistence (atomic writes)
# -----------------------------
def _atomic_write(path: str, content: str):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
 
 
def reset_txt_files():
    _atomic_write(PATH_TXT, "")
    _atomic_write(PUBLICATION_TXT, "")
 
 
def write_path_txt(sql_path: str):
    _atomic_write(PATH_TXT, f'file_path="{sql_path}"\n')
 
 
def read_path_txt():
    if not os.path.exists(PATH_TXT):
        return None
    raw = open(PATH_TXT, "r", encoding="utf-8").read().strip()
    if not raw:
        return None
    m = re.search(r'file_path\s*=\s*"(.*?)"', raw, re.IGNORECASE)
    return (m.group(1).strip() if m else raw.strip().strip('"'))
 
 
def write_publication_txt(pub_name: str):
    _atomic_write(PUBLICATION_TXT, f'publication_name="{pub_name}"\n')
 
 
def read_publication_txt():
    if not os.path.exists(PUBLICATION_TXT):
        return None
    raw = open(PUBLICATION_TXT, "r", encoding="utf-8").read().strip()
    if not raw:
        return None
    m = re.search(r'publication_name\s*=\s*"(.*?)"', raw, re.IGNORECASE)
    return (m.group(1).strip() if m else raw.strip().strip('"'))
 
 
def save_script1_context(context: dict):
    try:
        with open(LAST_SCRIPT1_CONTEXT_PATH, "w", encoding="utf-8") as f:
            json.dump(context, f, indent=2)
    except Exception:
        pass
 
 
# -----------------------------
# PowerShell runner helpers
# -----------------------------
def build_powershell_cmd(script_path: str, params: dict):
    cmd = [
        POWERSHELL_EXE,
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-File", script_path,
    ]
    for name, value in params.items():
        if value is not None and str(value).strip() != "":
            cmd.append(f"-{name}")
            cmd.append(str(value))
    return cmd
 
 
def sse(event: str, data: dict):
    return f"event: {event}\ndata: {json.dumps(data)}\n\n"
 
 
# -----------------------------
# Pages (under /replication/*)
# -----------------------------
@replication_bp.route("/")
def home():
    return render_template("home.html")
 
@replication_bp.route("/script1")
def script1():
    return render_template("script1.html")
 
@replication_bp.route("/script2")
def script2():
    return render_template("script2.html")
 
 
# -----------------------------
# APIs (under /replication/api/*)
# -----------------------------
@replication_bp.route("/api/script1/latest")
def api_script1_latest():
    return jsonify({
        "sql_file_path": read_path_txt(),
        "publication_name": read_publication_txt(),
        "path_txt_exists": os.path.exists(PATH_TXT),
        "publication_txt_exists": os.path.exists(PUBLICATION_TXT),
        "path_txt_mtime": os.path.getmtime(PATH_TXT) if os.path.exists(PATH_TXT) else None,
        "publication_txt_mtime": os.path.getmtime(PUBLICATION_TXT) if os.path.exists(PUBLICATION_TXT) else None,
    })
 
 
@replication_bp.route("/api/reinit_log/recent")
def api_reinit_log_recent():
    log_instance = (request.args.get("log_instance") or DEFAULT_LOG_INSTANCE or "").strip()
    log_database = (request.args.get("log_database") or DEFAULT_LOG_DATABASE or "").strip()
    log_table = (request.args.get("log_table") or REINIT_LOG_TABLE or "").strip()
    limit = int(request.args.get("limit") or 5)
    rows = fetch_recent_reinit_log(log_instance, log_database, log_table, limit=limit)
    return jsonify(rows)
 
 
@replication_bp.route("/api/repl_log/recent")
def api_repl_log_recent():
    log_instance = (request.args.get("log_instance") or DEFAULT_LOG_INSTANCE or "").strip()
    log_database = (request.args.get("log_database") or DEFAULT_LOG_DATABASE or "").strip()
    log_table = (request.args.get("log_table") or REPL_LOG_TABLE or "").strip()
    limit = int(request.args.get("limit") or 5)
    rows = fetch_recent_repl_log(log_instance, log_database, log_table, limit=limit)
    return jsonify(rows)
 
 
@replication_bp.route("/api/logs/data")
def api_logs_data():
    log_instance = (request.args.get("log_instance") or DEFAULT_LOG_INSTANCE or "").strip()
    log_database = (request.args.get("log_database") or DEFAULT_LOG_DATABASE or "").strip()
    repl_table = (request.args.get("repl_table") or REPL_LOG_TABLE or "repl_log").strip()
    reinit_table = (request.args.get("reinit_table") or REINIT_LOG_TABLE or "reinit_log").strip()
 
    repl_cols, repl_rows = fetch_full_table(log_instance, log_database, repl_table)
    reinit_cols, reinit_rows = fetch_full_table(log_instance, log_database, reinit_table)
 
    return jsonify({
        "log_instance": log_instance,
        "log_database": log_database,
        "repl_log": {"table": repl_table, "cols": repl_cols, "rows": repl_rows},
        "reinit_log": {"table": reinit_table, "cols": reinit_cols, "rows": reinit_rows},
    })
 
 
# -----------------------------
# RUN endpoints (under /replication/run/*)
# -----------------------------
@replication_bp.route("/run/script1", methods=["POST"])
def run_script1_stream():
    instance_name = (request.form.get("InstanceName") or "").strip()
    output_path = (request.form.get("OutputPath") or "").strip()
    tables_file_path = (request.form.get("TablesFilePath") or "").strip()
    reinit_instance = (request.form.get("ReinitInstance") or "").strip()
    log_instance = (request.form.get("LogInstance") or "").strip()
    log_database = (request.form.get("LogDatabase") or "").strip()
    log_table = (request.form.get("LogTable") or "").strip()
 
    params = {
        "InstanceName": instance_name,
        "OutputPath": output_path,
        "TablesFilePath": tables_file_path,
        "ReinitInstance": reinit_instance,
        "LogInstance": log_instance,
        "LogDatabase": log_database,
        "LogTable": log_table,
    }
 
    cmd = build_powershell_cmd(SCRIPT1_PATH, params)
    start_ts = time.time()
    started_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
 
    rx_pub_line = re.compile(r"---\s+Handling publication\s+'(?P<pub>[^']+)'", re.IGNORECASE)
    rx_init_file = re.compile(r"Initialized file:\s*(?P<path>.+?\.sql)\b", re.IGNORECASE)
    rx_scriptfile = re.compile(r"__SCRIPTFILE__:(?P<table>[^:]+):(?P<path>.+)$")
 
    latest_sql_path = None
    latest_pub = None
 
    def stream():
        nonlocal latest_sql_path, latest_pub
 
        try:
            reset_txt_files()
        except Exception:
            pass
 
        yield sse("meta", {"started_at": started_at})
 
        try:
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1
            )
        except Exception as e:
            yield sse("done", {"ok": False, "exit_code": 1, "stderr": str(e), "elapsed_sec": 0, "pct": 100})
            return
 
        try:
            for line in iter(p.stdout.readline, ""):
                if not line:
                    break
                line = line.rstrip("\n")
 
                m_pub = rx_pub_line.search(line)
                if m_pub:
                    latest_pub = m_pub.group("pub").strip()
                    try:
                        write_publication_txt(latest_pub)
                    except Exception:
                        pass
 
                m_init = rx_init_file.search(line)
                if m_init:
                    latest_sql_path = m_init.group("path").strip()
                    try:
                        write_path_txt(latest_sql_path)
                    except Exception:
                        pass
 
                m_sf = rx_scriptfile.search(line)
                if m_sf:
                    latest_sql_path = m_sf.group("path").strip()
                    try:
                        write_path_txt(latest_sql_path)
                    except Exception:
                        pass
 
                yield sse("line", {
                    "line": line,
                    "pct": 50,
                    "elapsed_sec": int(time.time() - start_ts)
                })
 
            p.wait()
 
        except Exception as e:
            yield sse("line", {"line": f"?? Streaming error: {e}", "pct": 90, "elapsed_sec": int(time.time() - start_ts)})
 
        stderr_txt = ""
        try:
            stderr_txt = (p.stderr.read() or "").strip()
        except Exception:
            pass
 
        code = p.returncode if p.returncode is not None else 1
        ok = (code == 0)
 
        save_script1_context({
            "stored_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "latest_sql_path": latest_sql_path,
            "latest_publication": latest_pub
        })
 
        yield sse("done", {
            "ok": ok,
            "exit_code": code,
            "stderr": stderr_txt,
            "elapsed_sec": int(time.time() - start_ts),
            "pct": 100,
            "latest_sql_path": latest_sql_path,
            "latest_publication": latest_pub
        })
 
    return Response(stream(), mimetype="text/event-stream")
 
 
@replication_bp.route("/run/script2", methods=["POST"])
def run_script2_stream():
    instance_name = (request.form.get("InstanceName") or "").strip()
    database_name = (request.form.get("DatabaseName") or "").strip()
 
    log_instance = (request.form.get("ReinitLogInstance") or "").strip()
    log_database = (request.form.get("ReinitLogDatabase") or "").strip()
    log_table = (request.form.get("ReinitLogTable") or "").strip()
 
    initiated_time = datetime.now()
    start_ts = time.time()
 
    sql_file_path = read_path_txt()
    publication_name = read_publication_txt()
 
    def fail(msg: str):
        yield sse("line", {"line": f"? {msg}", "pct": 100, "elapsed_sec": 0})
        yield sse("done", {"ok": False, "exit_code": 1, "stderr": msg, "elapsed_sec": 0, "pct": 100})
 
    if not instance_name or not database_name:
        return Response(fail("Missing InstanceName / DatabaseName"), mimetype="text/event-stream")
 
    if not log_instance or not log_database or not log_table:
        return Response(fail("Missing ReinitLogInstance / ReinitLogDatabase / ReinitLogTable"), mimetype="text/event-stream")
 
    if not sql_file_path:
        return Response(fail("path.txt is empty or missing SQL path. Run Script1 and ensure it prints 'Initialized file:' or '__SCRIPTFILE__'."), mimetype="text/event-stream")
 
    if not publication_name:
        return Response(fail("publication.txt is empty or missing publication name. Run Script1 and ensure it prints 'Handling publication'."), mimetype="text/event-stream")
 
    if not os.path.exists(sql_file_path):
        return Response(fail(f"SQL file not found: {sql_file_path}"), mimetype="text/event-stream")
 
    executed_by = os.getenv("USERNAME") or "unknown"
 
    cmd = build_powershell_cmd(SCRIPT2_PATH, {
        "InstanceName": instance_name,
        "SqlFilePath": sql_file_path,
        "DatabaseName": database_name,
 
        "ReinitLogInstance": log_instance,
        "ReinitLogDatabase": log_database,
        "ReinitLogTable": log_table,
 
        "ReinitializeInstance": instance_name,
        "PublicationName": publication_name,
        "ExecutedBy": executed_by,
        "InitiatedTime": initiated_time.strftime("%Y-%m-%d %H:%M:%S"),
    })
 
    def stream():
        yield sse("meta", {
            "initiated_time": initiated_time.strftime("%Y-%m-%d %H:%M:%S"),
            "sql_file_path": sql_file_path,
            "publication_name": publication_name
        })
        try:
            p = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                encoding="utf-8",
                errors="replace",
                bufsize=1
            )
        except Exception as e:
            yield sse("done", {"ok": False, "exit_code": 1, "stderr": str(e), "elapsed_sec": 0, "pct": 100})
            return
 
        try:
            for line in iter(p.stdout.readline, ""):
                if not line:
                    break
                line = line.rstrip("\n")
                yield sse("line", {"line": line, "pct": 50, "elapsed_sec": int(time.time() - start_ts)})
            p.wait()
        except Exception as e:
            yield sse("line", {"line": f"?? Streaming error: {e}", "pct": 90, "elapsed_sec": int(time.time() - start_ts)})
 
        stderr_txt = ""
        try:
            stderr_txt = (p.stderr.read() or "").strip()
        except Exception:
            pass
 
        code = p.returncode if p.returncode is not None else 1
        ok = (code == 0)
 
        yield sse("done", {"ok": ok, "exit_code": code, "stderr": stderr_txt, "elapsed_sec": int(time.time() - start_ts), "pct": 100})
 
    return Response(stream(), mimetype="text/event-stream")
 
 