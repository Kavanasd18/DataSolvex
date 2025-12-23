import os
import subprocess
from flask import Flask, request, render_template, jsonify, redirect, url_for, session
import pyodbc
import random
import string
from flask import Flask, render_template, jsonify, request, session, redirect, url_for
import pyodbc, os,re
import subprocess
from datetime import datetime, timedelta, timezone
import sqlite3
import time
from datetime import datetime
import shutil
import json
import logging
from flask import g
import time
from flask import template_rendered
from flask import has_request_context
from threading import local
import pandas as pd
from log_dash import create_log_dash


IST = timezone(timedelta(hours=5, minutes=30 ))

app = Flask(__name__)

# --- User Clone Management tool ---
from userclone_routes import userclone_bp
app.register_blueprint(userclone_bp, url_prefix='/userclone')

# --- SSIS Package Connection Updates tool ---
from ssis_routes import ssis_bp
app.register_blueprint(ssis_bp, url_prefix='/ssis')

app.secret_key = 'your_secret_key'  # Replace with a secure key
dash_app = create_log_dash(app)

# --------------------------------------------------
# Mount Inventory Management app under same port:
#   /inventory-mgmt/*
# --------------------------------------------------
from werkzeug.middleware.dispatcher import DispatcherMiddleware
from inventory_mgmt import get_inventory_app

_inventory_app = get_inventory_app(app.secret_key)
app.wsgi_app = DispatcherMiddleware(app.wsgi_app, {
    "/inventory-mgmt": _inventory_app
})


# --------------------------------------------------
# Mount Replication Reinitialization app under same port:
#   /replication-reinit/*
# --------------------------------------------------

from replication_reinitialization.replication_bp import replication_bp
app.register_blueprint(replication_bp,url_prefix="/replication")




# Admin credentials (static)
ADMIN_USERNAME = 'admin'
ADMIN_PASSWORD = 'Password123#'  # Replace with your admin password

# SQL Server connection details
def get_db_connection(server_name, db_name):
    print(f"Connecting to database: {db_name} on server: {server_name}")
    conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER={server_name};DATABASE={db_name};UID=EPMDBCCM;PWD=BujB8587*vvrwb')
    return conn


# Function to call the PowerShell script for validating the login
def validate_login_with_powershell(username, password):
    try:
        print(f"Running PowerShell script with username: {username} ")
        
        script_path = os.path.join(os.path.dirname(__file__), 'templates', 'validate_login.ps1')  # Path to your PowerShell script
        print(f"PowerShell script path: {script_path}")

        # Run PowerShell script with arguments
        result = subprocess.run(['powershell', '-ExecutionPolicy', 'Bypass', '-File', script_path, username, password],
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        print(f"PowerShell script output: {result.stdout}")
        print(f"PowerShell script error (if any): {result.stderr}")

        # Check if the login was successful based on PowerShell script output
        if 'Login Successful' in result.stdout:
            print("Login was successful according to PowerShell script.")
            return True
        else:
            print("Login failed according to PowerShell script output.")
            return False
    except Exception as e:
        print(f"Error running PowerShell script: {e}")
        return False

# Login route
@app.route('/')
def home():
    # If logged in, send them where they belong
    if session.get("login_name"):
        if session.get("is_admin"):
            return redirect(url_for("admin_home"))
        return redirect(url_for("index"))
    # Otherwise go login
    return redirect(url_for("login"))


@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        login_name = request.form.get('login_name', '').strip()
        login_password = request.form.get('password', '')

        print(f"Received login attempt: username = {login_name}")

        # Admin user check
        if login_name == ADMIN_USERNAME and login_password == ADMIN_PASSWORD:
            session.clear()
            session['login_name'] = login_name
            session["is_admin"] = True
            return redirect(url_for('admin_home'))   # ✅ admin goes to admin_home

        # Normal user check (PowerShell)
        if validate_login_with_powershell(login_name, login_password):
            session.clear()
            session['login_name'] = login_name
            session["is_admin"] = False
            return redirect(url_for('index'))        # ✅ normal user goes to main tool

        return render_template('login.html', error='Invalid Login Name or Password')

    return render_template('login.html')


@app.route('/index')
def index():
    # Check if user is logged in
    if 'login_name' in session:
        return render_template('index.html')
    else:
        # If not logged in, redirect to login page
        return redirect(url_for('login'))

@app.route("/admin-home")
def admin_home():
    if not session.get("login_name"):
        return redirect(url_for("login"))
    if not session.get("is_admin"):
        return redirect(url_for("index"))  # non-admin -> main tool
    return render_template("admin_home.html")



@app.route('/sqlserver_main')
def sqlserver_main():
    if 'login_name' in session:
        return render_template('sqlserver_main.html')
    else:
        return redirect(url_for('login'))





@app.route('/form')
def form():
    # Ensure user is logged in if needed
    if 'login_name' in session:
        return render_template('form.html')
    else:
        return redirect(url_for('login'))


@app.route('/inventory')
def inventory():
    # Keep legacy route but point to integrated app
    if 'login_name' in session:
        return redirect('/inventory-mgmt/')
    else:
        return redirect(url_for('login'))




# Function to generate the password
def generate_password():
    password = (
        random.choice(string.ascii_uppercase) +  # A random uppercase letter
        random.choice(string.ascii_lowercase) +  # A random lowercase letter
        random.choice(string.ascii_lowercase) +  # A random lowercase letter
        random.choice(string.ascii_uppercase) +  # A random uppercase letter
        str(random.randint(1000, 9999)) +         # A random 4-digit number
        random.choice('+-') +                    # A random special character
        random.choice(string.ascii_lowercase) +  # A random lowercase letter
        random.choice(string.ascii_lowercase) +  # A random lowercase letter
        random.choice(string.ascii_lowercase) +  # A random lowercase letter
        random.choice(string.ascii_lowercase) +  # A random lowercase letter
        random.choice(string.ascii_lowercase)    # A random lowercase letter
    )
    print(f"Generated password: {password}")
    return password


# Route to handle form submission for creating login
@app.route('/create-login', methods=['POST'])
def create_login():
    if 'login_name' not in session:
        return redirect(url_for('index'))  # Redirect to login if not logged in
    
    poc_manual = request.form.get('poc_manual','').strip()
    poc_select = request.form.get('poc_select','').strip()

    poc =poc_manual if poc_manual else poc_select
    # Fetching data from the form
    server_name = request.form['server_name']
    user_login_name = request.form['login_name']  # This is the login_name coming from the form
    request_id = request.form['request_id']
    application = request.form['application']
    database_name = request.form['database_name']
    user_name = request.form['user_name']
    environment = request.form['environment']
    reason = request.form['reason']  # New field for the reason for creating the login
    instance_name = server_name  # Automatically set instance_name to server_name
    product = request.form['product']
    Type = request.form['type']
    ownership = request.form['ownership']
    owner_contact = request.form['owner_contact']
    POC = request.form['poc']

    # Generate the password
    password = generate_password()
    created_date = 'GETDATE()'

    # Get the central database name from the form (it comes from the UI)
    central_db_name = 'EPMDBCCM'

    

    try:
        # 1st connection: For checking if the login already exists
        print(f"Connecting to source server: {server_name} to check if login already exists")
        conn = get_db_connection(server_name, 'master')  # Connect to the master DB to check logins
        cursor = conn.cursor()

        print(f"Checking if login {user_login_name} already exists")
        cursor.execute(f"SELECT 1 FROM sys.server_principals WHERE name = ?", (user_login_name,))
        result = cursor.fetchone()

        if result:
            return jsonify({'error': f"Login {user_login_name} already exists on the server."})
        
        # 2nd connection: For creating login and user in the target database
        print(f"Creating login: {user_login_name} with password: {password}")
        cursor.execute(f"CREATE LOGIN {user_login_name} WITH PASSWORD = '{password}';")
        
        print(f"Creating user: {user_name} for login: {user_login_name} in database: {database_name}")
        cursor.execute(f"USE {database_name}; CREATE USER {user_name} FOR LOGIN {user_login_name};")
        
        # 3rd connection: For inserting login details into the central database
        print(f"Connecting to central database: {central_db_name} on server: LAPTOP-3AU3RIT3")
        central_conn = pyodbc.connect(f'DRIVER={{SQL Server}};SERVER=LAPTOP-3AU3RIT3;DATABASE={central_db_name};UID=EPMDBCCM;PWD=BujB8587*vvrwb')
        central_cursor = central_conn.cursor()

        # Get the login_name from session (this is the actual logged-in user)
        created_by = session['login_name']  # This is the login name from the session

        print(f"Inserting login details into dbo.ID_repository_logindetails table")
        central_cursor.execute(f"""
            INSERT INTO dbo.ID_repository_logindetails (RequestID, ServerName, LoginName, UserName, Password, DatabaseName,
            Role, Application, CreatedDate, Reason, created_by, Product, Type, Ownership, Owner_Contact, PM_for_Application)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, {created_date}, ?, ?, ?,?,?,?,?)
        """, (request_id, server_name, user_login_name, user_name, password, database_name, 'Public', application,
              reason, created_by, product, Type, ownership, owner_contact, POC))

        # Commit the changes in the central database
        print("Committing changes to the central database.")
        central_conn.commit()

        central_conn.close()

        conn.commit()
        conn.close()

        script_path2 = r'E:\Login_AD_PS_1_working\templates\replica_login_sync.ps1'  # Raw string is okay
        print(f"PowerShell script path: {script_path2}")
        if not os.path.exists(script_path2):
            raise FileNotFoundError(f"PowerShell script not found at {script_path2}")
            # Run PowerShell script with arguments
        powershell_path = r"C:\Windows\System32\WindowsPowerShell\v1.0\powershell.exe"  # or wherever it exists on your machine

        print(server_name)
        print(user_login_name)



        result = subprocess.run([
            powershell_path,
            '-ExecutionPolicy', 'Bypass',
            '-File', script_path2,
            '-primaryServer', server_name,
            '-loginName', user_login_name
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )


        print("PowerShell Output:", result.stdout)
        #print("PowerShell Error:", result.stderr)

        if result.returncode != 0:
            raise RuntimeError(f"PowerShell script failed with exit code {result.returncode}")

        return jsonify({
            'message': f'Login {user_login_name} created successfully on {server_name}',
            'login_name': user_login_name,
            'powershell_output': result.stdout.strip()  # Optional: Send PowerShell output for debug/logging
        }), 200

    except Exception as e:
        # Log the error and return it to the client
        print(f"Error occurred: {str(e)}")
        return jsonify({'error': str(e)})
 

# Route to fetch databases based on the server name
@app.route('/fetch_databases', methods=['POST'])
def fetch_databases():
    try:
        server_name = request.form['server']
        ##print(f"Received request to fetch DBs from server: {server_name}")
        conn = get_db_connection(server_name, 'master')
        cursor = conn.cursor()

        cursor.execute("SELECT name FROM sys.databases;")
        databases = [row[0] for row in cursor.fetchall()]
        print(f"Databases fetched: {databases}")
        return jsonify({'databases': databases})
    except Exception as e:
        print(f"Error in fetch_databases: {e}")
        return jsonify({'error': str(e)})
    finally:
        conn.close()

# Route to check if a login already exists
@app.route('/check-login-exists', methods=['POST'])
def check_login_exists():
    login_name = request.form['login_name']
    server_name = request.form['server_name']

    conn = get_db_connection(server_name, 'master')  # Using master DB for checking logins
    cursor = conn.cursor()

    try:
        # Check if the login exists
        cursor.execute(f"SELECT 1 FROM sys.server_principals WHERE name = ?", (login_name,))
        result = cursor.fetchone()

        if result:
            return jsonify({'exists': True})
        else:
            return jsonify({'exists': False})

    except Exception as e:
        logging.error(f"Error checking if login exists: {e}")
        return jsonify({'error': str(e)})
    finally:
        conn.close() 





@app.route("/loganalytics")
def loganalytics_root():
    return redirect("/loganalytics/")


@app.before_request
def protect_loganalytics():
    path = request.path or ""
    # Only protect the Dash area
    if path.startswith("/loganalytics"):
        if not session.get("is_admin"):
            return redirect(url_for("login"))



 
# LDAP validation function
def validate_login(username, password):
    # Add the static prefix 'ITLinfosys\\' to the username
    full_username = f"ITLinfosys\\{username}"
 
    try:
        # Connect to the LDAP server to validate user credentials
        ldap_server = 'ldap://BLRKECIDC05.ad.infosys.com:389'  # Use ldaps:// for a secure connection (port 636)
        server = Server(ldap_server, get_info=ALL, use_ssl=False)  # Set use_ssl=True for LDAPS
        connection = Connection(server, user=full_username, password=password, auto_bind=True)
 
        # If the connection is successful, the login is valid
        if connection.bound:
            return True
        else:
            return False
 
    except Exception as e:
        print(f"An error occurred during login validation: {e}")
        return False
 

@app.route('/dbrefresh')
def dbrefresh():
    if 'login_name' not in session:
        return redirect(url_for('dbpage'))  # Redirect to login if not logged in
    return render_template('dbpage.html')



# Helper: Get client IP
def get_user_ip():
    forwarded = request.environ.get('HTTP_X_FORWARDED_FOR')
    if forwarded:
        ip = forwarded.split(',')[0].strip()
    else:
        ip = request.remote_addr
    return ip

def normalize_page(page):
    if not page:
        return '/'
    page = page.strip().lower()
    if page.endswith('/'):
        page = page[:-1]
    if page == '':
        page = '/'
    # add .html if no extension (and not root)
    if page != '/' and '.' not in page:
        page += '.html'
    return page


@app.route('/log-entry', methods=['POST'])
def log_entry():
    try:
        data = request.get_json(force=True)
        page = normalize_page(data.get('page'))
        timestamp_str = data.get('timestamp')
 
        # Parse ISO timestamp in UTC (e.g., "2025-09-17T10:44:21.650Z")
        utc_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
 
        # Convert to IST
        ist_time = utc_time.astimezone(IST)
 
        # Strip timezone info to store as naive datetime in SQL Server
        enter_time = ist_time.replace(tzinfo=None)
        user = session.get('login_name', 'anonymous')
        ip = get_user_ip()

        print(f"Entry received for {user} -> {page} at {enter_time} (IST)")

        conn = get_sql_server_connection()
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO page_access_logs (login_name, page, ip_address, enter_time, exit_time, duration_seconds)
            VALUES (?, ?, ?, ?, NULL, NULL)
        """, user, page, ip, enter_time)

        conn.commit()
        conn.close()
        return jsonify({'status': 'ok'})
    except Exception as e:
        print(f"Error logging entry: {e}")
        return jsonify({'error': str(e)}), 500


@app.route('/log-exit', methods=['POST'])
def log_exit():
    try:
        data = request.get_json(force=True)
        page = data.get('page')
        page = normalize_page(page)
        timestamp_str = data.get('timestamp')
 
        # Parse ISO timestamp in UTC (e.g., "2025-09-17T10:44:21.650Z")
        utc_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
 
        # Convert to IST
        ist_time = utc_time.astimezone(IST)
 
        # Strip timezone info to store as naive datetime in SQL Server
        exit_time = ist_time.replace(tzinfo=None)
        #timestamp_str = data.get('timestamp')
       #exit_time = datetime.fromisoformat(timestamp_str.replace('Z', ''))  # remove Z if needed
        user = session.get('login_name', 'anonymous')

        print(f"Exit received for {user} -> {page} at {exit_time}")

        conn = get_sql_server_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT TOP 1 id, enter_time
            FROM page_access_logs
            WHERE login_name = ? AND page = ? AND exit_time IS NULL
            ORDER BY id DESC
        """, user, page)

        row = cursor.fetchone()
        if row:
            log_id, enter_time = row
            duration = (exit_time - enter_time).total_seconds()
            print(f"Updating log id {log_id}: duration = {duration} seconds")

            cursor.execute("""
                UPDATE page_access_logs
                SET exit_time = ?, duration_seconds = ?
                WHERE id = ?
            """, exit_time, duration, log_id)

            conn.commit()
        else:
            print(f"No open log found for {user} on {page}")

        conn.close()
        return jsonify({'status': 'ok'})
    except Exception as e:
        print(f"Error logging exit: {e}")
        return jsonify({'error': str(e)}), 500

# SQL Server connection helper
def get_sql_server_connection():
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=LAPTOP-3AU3RIT3;"
        "DATABASE=dbrefresh;"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

# Initialize database and tables
def init_db():
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'page_access_logs') AND type = 'U')
    BEGIN
        CREATE TABLE page_access_logs (
        id INT IDENTITY(1,1) PRIMARY KEY,
        login_name NVARCHAR(100) NOT NULL,
        page NVARCHAR(255) NOT NULL,
        ip_address NVARCHAR(50) NOT NULL,
        enter_time DATETIME NOT NULL,
        exit_time DATETIME NULL,
        duration_seconds FLOAT NULL
    );

    CREATE INDEX idx_page_access_logs_user_page_time
    ON page_access_logs (login_name, page, enter_time DESC);

    END
    """)

    # Create sessions table if not exists
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'sessions') AND type = 'U')
    BEGIN
        CREATE TABLE sessions (
            id INT IDENTITY(1,1) PRIMARY KEY,
            ip NVARCHAR(MAX),
            state_json NVARCHAR(MAX),
            server_name NVARCHAR(MAX),
            database_name NVARCHAR(MAX),
            folder_path NVARCHAR(MAX),
            destination_server NVARCHAR(MAX),
            destination_database NVARCHAR(MAX),
            step10_tablename_files NVARCHAR(MAX),
            step1_errors_folder NVARCHAR(MAX),
            step2_errors_folder NVARCHAR(MAX),
            step3_errors_folder NVARCHAR(MAX),
            step4_errors_folder NVARCHAR(MAX),
            step5_errors_folder NVARCHAR(MAX),
            step6_errors_folder NVARCHAR(MAX),
            step7_errors_folder NVARCHAR(MAX),
            step8_errors_folder NVARCHAR(MAX),
            step9_errors_folder NVARCHAR(MAX),
            step10_errors_folder NVARCHAR(MAX),
            step11_errors_folder NVARCHAR(MAX),
            step12_errors_folder NVARCHAR(MAX),
            step13_errors_folder NVARCHAR(MAX),
            step14_errors_folder NVARCHAR(MAX),
            folder_created BIT,
            validation_folder_path NVARCHAR(MAX),
            validation_folder_path1 NVARCHAR(MAX),
            validation_folder_path2 NVARCHAR(MAX),
            validation_folder_path3 NVARCHAR(MAX),
            validation_folder_path4 NVARCHAR(MAX),
            validation_folder_path5 NVARCHAR(MAX),
            validation_folder_path6 NVARCHAR(MAX),
            validation_folder_path7 NVARCHAR(MAX),
            validation_folder_path8 NVARCHAR(MAX),
            validation_folder_path9 NVARCHAR(MAX),
            validation_folder_path10 NVARCHAR(MAX),
            validation_folder_path11 NVARCHAR(MAX),
            validation_folder_path12 NVARCHAR(MAX),
            validation_folder_path13 NVARCHAR(MAX),
            validation_folder_path14 NVARCHAR(MAX),
            scripts_folder_path NVARCHAR(MAX),
            corp_names_folder_path NVARCHAR(MAX),
            corp_objects_folder_path NVARCHAR(MAX),
            corp_list NVARCHAR(MAX),
            schemanamefrom NVARCHAR(MAX),
            schemanameto NVARCHAR(MAX)
            

        )
    END
    """)

    # Create autosave table if not exists
    cursor.execute("""
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'autosave') AND type = 'U')
    BEGIN
        CREATE TABLE autosave (
            id INT IDENTITY(1,1) PRIMARY KEY,
            
            database_name NVARCHAR(255),
            state_json NVARCHAR(MAX),
            CONSTRAINT UQ_autosave_ip_db UNIQUE ( database_name)
        )
    END
    """)

    conn.commit()
    conn.close()


# Autosave state
@app.route("/autosave", methods=["POST"])
def autosave():
    data = request.json
    db_name = data.get("database_name")
 
    if not db_name:
        return jsonify({"error": "Database name is required"}), 400
 
    conn = get_sql_server_connection()
    cursor = conn.cursor()
 
    # Update or insert autosave state using only database_name
    cursor.execute("SELECT id FROM autosave WHERE database_name = ?", db_name)
    row = cursor.fetchone()
    if row:
        cursor.execute("UPDATE autosave SET state_json = ? WHERE database_name = ?", json.dumps(data), db_name)
    else:
        cursor.execute("INSERT INTO autosave (database_name, state_json) VALUES (?, ?)", db_name, json.dumps(data))
 
    conn.commit()
    conn.close()
    return jsonify({"status": "saved"})

# Recover state
@app.route("/recover_state", methods=["GET"])
def recover_state():
    user_ip = get_user_ip()
    db_name = request.args.get("database_name")

    if not db_name:
        return jsonify({"error": "Database name is required"}), 400

    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT state_json FROM autosave WHERE  database_name = ?",  db_name)
    row = cursor.fetchone()
    conn.close()

    if row and row[0]:
        return jsonify(json.loads(row[0]))
    return jsonify({})

# Delete progress
@app.route("/delete_progress", methods=["POST"])
def delete_progress():
    data = request.json
    db_name = data.get("database_name")
 
    if not db_name:
        return jsonify({"error": "Database name is required"}), 400
 
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM autosave WHERE database_name = ?", db_name)
    conn.commit()
    conn.close()
    return jsonify({"status": "deleted"}) 






# Function to get databases dynamically from a given server
def get_databases_from_server(server):
    try:
        # Build the connection string for SQL Server with Windows Authentication
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # Query to get the list of databases (excluding system databases)
        cursor.execute("SELECT name FROM sys.databases WHERE state_desc = 'ONLINE' AND name NOT IN ('master', 'tempdb', 'model', 'msdb')")
        databases = [row[0] for row in cursor.fetchall()]

        cursor.close()
        conn.close()

        return databases
    except Exception as e:
        return str(e)

@app.route('/dbpage')
def dbpage():
    return render_template('dbpage.html')

@app.route('/search_servers', methods=['GET'])
def search_servers():
    search_query = request.args.get('query', '').lower()  # Case-insensitive search query
    
    if search_query:
        databases = get_databases_from_server(search_query)
        
        if isinstance(databases, list):  # If successful, return the databases ["List of Databases"] +
            return jsonify({search_query:  databases})
        else:
            return jsonify({search_query: []}), 400  # If error, return empty list with error status

    return jsonify({"error": "No server provided."}), 400

@app.route('/submit_folder', methods=['POST'])
def submit_folder():
    server_name = request.form.get('server')
    database_name = request.form.get('database')
    folder_path = request.form.get('folder_path')
    destination_server = request.form.get('destination_server')
    destination_database = request.form.get('destination_database')
    step10_table_files = request.form.get('step10_tablename_files')
    schemanamefrom = request.form.get('schemanamefrom')
    schemanameto = request.form.get('schemanameto')
    print(server_name)
    powershell_script_path = r".\scripts\step1.ps1"
    powershell_script_path2 = r".\scripts\step2.ps1"
    powershell_script_path3 = r".\scripts\step3.ps1"
    powershell_script_path4 = r".\scripts\step4.ps1"
    powershell_script_path5 = r".\scripts\step5.ps1"
    powershell_script_path6 = r".\scripts\step6.ps1"
    powershell_script_path7 = r".\scripts\step7.ps1"
    powershell_script_path8 = r".\scripts\step8.ps1"
    powershell_script_path9 = r".\scripts\step9.ps1"
    powershell_script_path10 = r".\scripts\step10.ps1"
    powershell_script_path12 = r".\scripts\step12.ps1"
    powershell_script_path13 = r".\scripts\step13.ps1"
    
    # Extract script name from the path
    script_name1 = os.path.basename(powershell_script_path)
    script_name2 = os.path.basename(powershell_script_path2)
    script_name3 = os.path.basename(powershell_script_path3)
    script_name4 = os.path.basename(powershell_script_path4)
    script_name5 = os.path.basename(powershell_script_path5)
    script_name6 = os.path.basename(powershell_script_path6)
    script_name7 = os.path.basename(powershell_script_path7)
    script_name8 = os.path.basename(powershell_script_path8)
    script_name9 = os.path.basename(powershell_script_path9)
    script_name10 = os.path.basename(powershell_script_path10)
    script_name11 = "step11"
    script_name12 = os.path.basename(powershell_script_path12)
    script_name13 = os.path.basename(powershell_script_path13)
    script_name14 = "step14"


    if server_name and database_name and folder_path:
        # Create a subfolder inside the given folder path named after the database
        database_folder_path = os.path.join(folder_path, database_name)
        

        # Ensure the database folder exists
        if not os.path.exists(database_folder_path):
            os.makedirs(database_folder_path)
            # Create ErrorLogs folder inside the database folder
            error_logs_folder_path = os.path.join(database_folder_path, 'ErrorLogs')
            os.makedirs(error_logs_folder_path)
            

            scripts_folder_path = os.path.join(database_folder_path, 'scripts')
            os.makedirs(scripts_folder_path)

            #create the validation folder
            validation_folder_path = os.path.join(database_folder_path, 'validation')
            os.makedirs(validation_folder_path)

            corp_names_folder_path = os.path.join(database_folder_path, 'corp_names')
            os.makedirs(corp_names_folder_path)

            corp_objects_folder_path = os.path.join(database_folder_path, 'corpuser_objects')
            os.makedirs(corp_objects_folder_path)

            corp_list = os.path.join(database_folder_path, 'corpuser_list')
            os.makedirs(corp_list)

            validation_folder_path1 = os.path.join(validation_folder_path, script_name1)
            os.makedirs(validation_folder_path1)

            validation_folder_path2 = os.path.join(validation_folder_path, script_name2)
            os.makedirs(validation_folder_path2)
            
            validation_folder_path3 = os.path.join(validation_folder_path, script_name3)
            os.makedirs(validation_folder_path3)

            validation_folder_path4 = os.path.join(validation_folder_path, script_name4)
            os.makedirs(validation_folder_path4)

            validation_folder_path5 = os.path.join(validation_folder_path, script_name5)
            os.makedirs(validation_folder_path5)

            validation_folder_path6 = os.path.join(validation_folder_path, script_name6)
            os.makedirs(validation_folder_path6)

            validation_folder_path7 = os.path.join(validation_folder_path, script_name7)
            os.makedirs(validation_folder_path7)

            validation_folder_path8 = os.path.join(validation_folder_path, script_name8)
            os.makedirs(validation_folder_path8)
            
            validation_folder_path9 = os.path.join(validation_folder_path, script_name9)
            os.makedirs(validation_folder_path9)


            validation_folder_path10 = os.path.join(validation_folder_path, script_name10)
            os.makedirs(validation_folder_path10)


            validation_folder_path11 = os.path.join(validation_folder_path, script_name11)
            os.makedirs(validation_folder_path11)
            
            validation_folder_path12 = os.path.join(validation_folder_path, script_name12)
            os.makedirs(validation_folder_path12)
            
            validation_folder_path13 = os.path.join(validation_folder_path, script_name13)
            os.makedirs(validation_folder_path13)

            validation_folder_path14 = os.path.join(validation_folder_path, script_name14)
            os.makedirs(validation_folder_path14)


            # Create SchemaErrors folder inside the ErrorLogs folder
            step1_errors_folder_path = os.path.join(error_logs_folder_path, 'step1')
            os.makedirs(step1_errors_folder_path)
            
            # Create DbErrors folder inside the ErrorLogs folder
            step2_errors_folder_path = os.path.join(error_logs_folder_path, 'step2')
            os.makedirs(step2_errors_folder_path)
            
                        # Create SchemaErrors folder inside the ErrorLogs folder
            step3_errors_folder_path = os.path.join(error_logs_folder_path, 'step3')
            os.makedirs(step3_errors_folder_path)
            
            # Create DbErrors folder inside the ErrorLogs folder
            step4_errors_folder_path = os.path.join(error_logs_folder_path, 'step4')
            os.makedirs(step4_errors_folder_path)
            

            # Create DbErrors folder inside the ErrorLogs folder
            step5_errors_folder_path = os.path.join(error_logs_folder_path, 'step5')
            os.makedirs(step5_errors_folder_path)

            step6_errors_folder_path = os.path.join(error_logs_folder_path, 'step6')
            os.makedirs(step6_errors_folder_path)

            step7_errors_folder_path = os.path.join(error_logs_folder_path, 'step7')
            os.makedirs(step7_errors_folder_path)

            step8_errors_folder_path = os.path.join(error_logs_folder_path, 'step8')
            os.makedirs(step8_errors_folder_path)

            step9_errors_folder_path = os.path.join(error_logs_folder_path, 'step9')
            os.makedirs(step9_errors_folder_path)

            step10_errors_folder_path = os.path.join(error_logs_folder_path, 'step10')
            os.makedirs(step10_errors_folder_path)

            step11_errors_folder_path = os.path.join(error_logs_folder_path, 'step11')
            os.makedirs(step11_errors_folder_path)

            step12_errors_folder_path = os.path.join(error_logs_folder_path, 'step12')
            os.makedirs(step12_errors_folder_path)

            step13_errors_folder_path = os.path.join(error_logs_folder_path, 'step13')
            os.makedirs(step13_errors_folder_path)

            step14_errors_folder_path = os.path.join(error_logs_folder_path, 'step14')
            os.makedirs(step14_errors_folder_path)


            conn = get_sql_server_connection()
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO sessions 
                        (server_name, database_name, folder_path,destination_server,destination_database,step10_tablename_files,
                         step1_errors_folder, step2_errors_folder,step3_errors_folder, step4_errors_folder,step5_errors_folder,
                         step6_errors_folder,step7_errors_folder,step8_errors_folder,step9_errors_folder,step10_errors_folder,
                         step11_errors_folder,step12_errors_folder,step13_errors_folder,step14_errors_folder, folder_created,
                        validation_folder_path, validation_folder_path1, validation_folder_path2, validation_folder_path3, 
                        validation_folder_path4, validation_folder_path5,validation_folder_path6,validation_folder_path7,
                        validation_folder_path8,validation_folder_path9,validation_folder_path10,validation_folder_path11,
                        validation_folder_path12,validation_folder_path13,validation_folder_path14, scripts_folder_path, corp_names_folder_path,
                        corp_objects_folder_path,corp_list, schemanamefrom, schemanameto)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?,?, ?, ?, ?, ?, ?, ?)''',
                    (server_name, database_name, folder_path,destination_server,destination_database,step10_table_files, 
                    step1_errors_folder_path, step2_errors_folder_path, step3_errors_folder_path, step4_errors_folder_path,
                    step5_errors_folder_path,step6_errors_folder_path,step7_errors_folder_path,step8_errors_folder_path,
                    step9_errors_folder_path,step10_errors_folder_path,step11_errors_folder_path,step12_errors_folder_path,
                    step13_errors_folder_path,step14_errors_folder_path, True,
                    validation_folder_path, validation_folder_path1, validation_folder_path2, validation_folder_path3,
                    validation_folder_path4, validation_folder_path5, validation_folder_path6,validation_folder_path7,
                    validation_folder_path8,validation_folder_path9,validation_folder_path10,validation_folder_path11,
                    validation_folder_path12, validation_folder_path13,validation_folder_path14, scripts_folder_path,corp_names_folder_path,
                    corp_objects_folder_path,corp_list,schemanamefrom,schemanameto))
            conn.commit()
            conn.close()

            return jsonify({"status": f"Folder {database_folder_path} created with ErrorLogs, SchemaErrors, and DbErrors folders."})
        else:
            # Check if ErrorLogs subfolder exists
            error_logs_folder_path = os.path.join(database_folder_path, 'ErrorLogs')
            if not os.path.exists(error_logs_folder_path):
                os.makedirs(error_logs_folder_path)

            # Check if validation subfolder exists
            validation_folder_path = os.path.join(database_folder_path, 'Validation')
            if not os.path.exists(validation_folder_path):
                os.makedirs(validation_folder_path)

            scripts_folder_path = os.path.join(database_folder_path, 'scripts')
            if not os.path.exists(scripts_folder_path):
                os.makedirs(scripts_folder_path)
            
            corp_names_folder_path = os.path.join(database_folder_path, 'corp_names')
            if not os.path.exists(corp_names_folder_path):    
                os.makedirs(corp_names_folder_path)
            


            corp_objects_folder_path = os.path.join(database_folder_path, 'corpuser_objects')
            if not os.path.exists(corp_objects_folder_path):    
                os.makedirs(corp_objects_folder_path)

            corp_list = os.path.join(database_folder_path, 'corp_list')
            if not os.path.exists(corp_list):    
                os.makedirs(corp_list)


            validation_folder_path1 = os.path.join(validation_folder_path, script_name1)
            if not os.path.exists(validation_folder_path1):
                os.makedirs(validation_folder_path1)



            validation_folder_path2 = os.path.join(validation_folder_path, script_name2)
            if not os.path.exists(validation_folder_path2):
                os.makedirs(validation_folder_path2)

            validation_folder_path3 = os.path.join(validation_folder_path, script_name3)
            if not os.path.exists(validation_folder_path3):
                os.makedirs(validation_folder_path3)
            
            validation_folder_path4 = os.path.join(validation_folder_path, script_name4)
            if not os.path.exists(validation_folder_path4):
                os.makedirs(validation_folder_path4)

            validation_folder_path5 = os.path.join(validation_folder_path, script_name5)
            if not os.path.exists(validation_folder_path5):
                os.makedirs(validation_folder_path5)
            

            validation_folder_path6 = os.path.join(validation_folder_path, script_name6)
            if not os.path.exists(validation_folder_path6):
                os.makedirs(validation_folder_path6)
            
            validation_folder_path7 = os.path.join(validation_folder_path, script_name7)
            if not os.path.exists(validation_folder_path7):
                os.makedirs(validation_folder_path7)

            validation_folder_path8 = os.path.join(validation_folder_path, script_name8)
            if not os.path.exists(validation_folder_path8):
                os.makedirs(validation_folder_path8)
            
            validation_folder_path9 = os.path.join(validation_folder_path, script_name9)
            if not os.path.exists(validation_folder_path9):
                os.makedirs(validation_folder_path9)
            
            validation_folder_path10 = os.path.join(validation_folder_path, script_name10)
            if not os.path.exists(validation_folder_path10):
                os.makedirs(validation_folder_path10)
            
            validation_folder_path11 = os.path.join(validation_folder_path, script_name11)
            if not os.path.exists(validation_folder_path11):
                os.makedirs(validation_folder_path11)

            validation_folder_path12 = os.path.join(validation_folder_path, script_name12)
            if not os.path.exists(validation_folder_path12):
                os.makedirs(validation_folder_path12)

            validation_folder_path13 = os.path.join(validation_folder_path, script_name13)
            if not os.path.exists(validation_folder_path13):
                os.makedirs(validation_folder_path13)

            validation_folder_path14 = os.path.join(validation_folder_path, script_name14)
            if not os.path.exists(validation_folder_path14):
                os.makedirs(validation_folder_path14)



            # Check if SchemaErrors subfolder exists
            step1_errors_folder_path = os.path.join(error_logs_folder_path, 'step1')
            if not os.path.exists(step1_errors_folder_path):
                os.makedirs(step1_errors_folder_path)

            # Check if DbErrors subfolder exists
            step2_errors_folder_path = os.path.join(error_logs_folder_path, 'step2')
            if not os.path.exists(step2_errors_folder_path):
                os.makedirs(step2_errors_folder_path)
            
            step3_errors_folder_path = os.path.join(error_logs_folder_path, 'step3')
            if not os.path.exists(step3_errors_folder_path):
                os.makedirs(step3_errors_folder_path)

            # Check if DbErrors subfolder exists
            step4_errors_folder_path = os.path.join(error_logs_folder_path, 'step4')
            if not os.path.exists(step4_errors_folder_path):
                os.makedirs(step4_errors_folder_path)
            

            step5_errors_folder_path = os.path.join(error_logs_folder_path, 'step5')
            if not os.path.exists(step5_errors_folder_path):
                os.makedirs(step5_errors_folder_path)

            step6_errors_folder_path = os.path.join(error_logs_folder_path, 'step6')
            if not os.path.exists(step6_errors_folder_path):
                os.makedirs(step6_errors_folder_path)
            
            step7_errors_folder_path = os.path.join(error_logs_folder_path, 'step7')
            if not os.path.exists(step7_errors_folder_path):
                os.makedirs(step7_errors_folder_path)
            
            step8_errors_folder_path = os.path.join(error_logs_folder_path, 'step8')
            if not os.path.exists(step8_errors_folder_path):
                os.makedirs(step8_errors_folder_path)
            
            step9_errors_folder_path = os.path.join(error_logs_folder_path, 'step9')
            if not os.path.exists(step9_errors_folder_path):
                os.makedirs(step9_errors_folder_path)
            

            step10_errors_folder_path = os.path.join(error_logs_folder_path, 'step10')
            if not os.path.exists(step10_errors_folder_path):
                os.makedirs(step10_errors_folder_path)
            
            step11_errors_folder_path = os.path.join(error_logs_folder_path, 'step11')
            if not os.path.exists(step11_errors_folder_path):
                os.makedirs(step11_errors_folder_path)
            
            step12_errors_folder_path = os.path.join(error_logs_folder_path, 'step12')
            if not os.path.exists(step12_errors_folder_path):
                os.makedirs(step12_errors_folder_path)

            step13_errors_folder_path = os.path.join(error_logs_folder_path, 'step13')
            if not os.path.exists(step13_errors_folder_path):
                os.makedirs(step13_errors_folder_path)

            step14_errors_folder_path = os.path.join(error_logs_folder_path, 'step14')
            if not os.path.exists(step14_errors_folder_path):
                os.makedirs(step14_errors_folder_path)


            conn = get_sql_server_connection()
            cursor = conn.cursor()
            cursor.execute('''INSERT INTO sessions 
                        (server_name, database_name, folder_path,destination_server,destination_database,step10_tablename_files,
                         step1_errors_folder, step2_errors_folder,step3_errors_folder, step4_errors_folder,step5_errors_folder,
                         step6_errors_folder,step7_errors_folder,step8_errors_folder,step9_errors_folder,step10_errors_folder,
                         step11_errors_folder,step12_errors_folder,step13_errors_folder,step14_errors_folder, folder_created,
                        validation_folder_path, validation_folder_path1, validation_folder_path2, validation_folder_path3, 
                        validation_folder_path4, validation_folder_path5,validation_folder_path6,validation_folder_path7,
                        validation_folder_path8,validation_folder_path9,validation_folder_path10,validation_folder_path11,
                        validation_folder_path12,validation_folder_path13,validation_folder_path14, scripts_folder_path, corp_names_folder_path,
                        corp_objects_folder_path,corp_list, schemanamefrom, schemanameto)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                        ?,?, ?, ?, ?, ?, ?, ?)''',
                    (server_name, database_name, folder_path,destination_server,destination_database,step10_table_files, 
                    step1_errors_folder_path, step2_errors_folder_path, step3_errors_folder_path, step4_errors_folder_path,
                    step5_errors_folder_path,step6_errors_folder_path,step7_errors_folder_path,step8_errors_folder_path,
                    step9_errors_folder_path,step10_errors_folder_path,step11_errors_folder_path,step12_errors_folder_path,
                    step13_errors_folder_path,step14_errors_folder_path, True,
                    validation_folder_path, validation_folder_path1, validation_folder_path2, validation_folder_path3,
                    validation_folder_path4, validation_folder_path5, validation_folder_path6,validation_folder_path7,
                    validation_folder_path8,validation_folder_path9,validation_folder_path10,validation_folder_path11,
                    validation_folder_path12, validation_folder_path13,validation_folder_path14, scripts_folder_path,corp_names_folder_path,
                    corp_objects_folder_path,corp_list,schemanamefrom,schemanameto))
            conn.commit()
            conn.close()


            # Store the paths of both SchemaErrors and DbErrors folders in the session
            #session['schema_errors_folder'] = schema_errors_folder_path
            #session['validation_folder_path'] = validation_folder_path
            #session['validation_folder_path1'] = validation_folder_path1
            #session['validation_folder_path2'] = validation_folder_path2
            #session['validation_folder_path3'] = validation_folder_path3
            #session['validation_folder_path4'] = validation_folder_path4
            #session['validation_folder_path5'] = validation_folder_path5
            #session['db_errors_folder'] = db_errors_folder_path
            #session['scripts_folder_path'] = scripts_folder_path
            #session['folder_created'] = True  # Store that the folder already exists
            return jsonify({"status": f"Folder {database_folder_path} already exists with ErrorLogs, SchemaErrors, and DbErrors folders."})

    return jsonify({"error": "Missing parameters: server, database, or folder path"}), 400


@app.route('/run_powershell1', methods=['GET'])
def run_powershell1():
    # Get parameters from the request
    database_name = request.args.get('database')
    try:
        # Database connection
        conn = get_sql_server_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT server_name, step1_errors_folder, scripts_folder_path, validation_folder_path1 
            FROM sessions 
            WHERE database_name = ? AND folder_created = 1
        ''', (database_name,))

        session_data = cursor.fetchone()
        conn.close()


        if not session_data:
            return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

        
        # Unpack the session data
        server_name, step1_errors_folder_path, scripts_folder_path, validation_folder_path1 = session_data
        print(server_name, step1_errors_folder_path, scripts_folder_path, validation_folder_path1 )
        powershell_script_path = r".\scripts\step1.ps1"

        if server_name and database_name and step1_errors_folder_path:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            log_file_path = os.path.join(step1_errors_folder_path, f"{database_name}_step1_errors_{timestamp}.txt")
            validation_file_path = os.path.join(validation_folder_path1, f"{database_name}_Validation_step1_report_{timestamp}.txt")

            try:
                start_time = time.time()

                # Run PowerShell script
                process = subprocess.Popen(
                    ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                     "-serverName", server_name, "-databaseName", database_name,
                     "-outputRootFolder", step1_errors_folder_path, "-viewsFolder", "Views", "-outputFile", log_file_path, "-viewNamesFile", log_file_path],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )

                stdout, stderr = process.communicate()
                stderr = stderr.decode("utf-8")

                if stderr:
                    with open(log_file_path, 'a') as log_file:
                        log_file.write("\nPowerShell Errors:\n")
                        log_file.write(stderr)

                    end_time = time.time()
                    time_taken = end_time - start_time

                    # Return error status
                    return jsonify({
                        "status": "Error running PowerShell",
                        "log_file": log_file_path,
                        "stderr": stderr,
                        "time_taken": time_taken,
                        "minutes": int(time_taken // 60),
                        "seconds": int(time_taken % 60),
                        "milliseconds": int((time_taken * 1000) % 1000)
                    })

                # SQL Query to check for schema issues
                conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()
                cursor.execute("SELECT count(*) FROM sys.views WHERE OBJECTPROPERTY(object_id, 'IsSchemaBound') = 1")
                result = cursor.fetchone()[0]

                # Create validation file based on the schema check result
                with open(validation_file_path, 'a') as validation_file:
                    if result == 0:
                        validation_file.write(f"SQL Query Result: {result} \n")
                        validation_file.write(f"No objects found with 'testowner' schema.\n")
                        end_time = time.time()
                        time_taken = end_time - start_time
                        return jsonify({
                            "status": "PowerShell script executed successfully. No errors in the schema.",
                            "validation_file": validation_file_path,
                            "time_taken": time_taken,
                            "minutes": int(time_taken // 60),
                            "seconds": int(time_taken % 60),
                            "milliseconds": int((time_taken * 1000) % 1000)
                        })
                    else:
                        validation_file.write(f"SQL Query Result: {result} \n")
                        validation_file.write(f"Schema issue detected.\n")
                        end_time = time.time()
                        time_taken = end_time - start_time
                        return jsonify({
                            "status": f"Still there are {result} schema bound objects in testonwer.",
                            "log_file": log_file_path,
                            "time_taken": time_taken,
                            "minutes": int(time_taken // 60),
                            "seconds": int(time_taken % 60),
                            "milliseconds": int((time_taken * 1000) % 1000)
                        })

            except Exception as e:
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nException:\n")
                    log_file.write(str(e))
                return jsonify({"error": f"An error occurred: {str(e)}"})

        else:
            return jsonify({"error": "Missing parameters: server, database, or folder path"}), 400

    except sqlite3.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500



@app.route('/run_powershell2', methods=['GET'])
def run_powershell2():
    database_name = request.args.get('database')

    # Check if folder has been created
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()

        cursor.execute('''
            SELECT server_name, step2_errors_folder, validation_folder_path2 
            FROM sessions 
            WHERE database_name = ? AND folder_created = 1
        ''', (database_name,))

        session_data = cursor.fetchone()
        conn.close()


        if session_data:
            server_name, step2_errors_folder_path, validation_folder_path2 = session_data
            print(server_name, step2_errors_folder_path, validation_folder_path2)
        else:
            return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    except sqlite3.Error as e:
        print(f"Database error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        print(f"General error: {str(e)}")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    # PowerShell script path
    powershell_script_path = r".\scripts\step2.ps1"

    if server_name and database_name and step2_errors_folder_path:
        # Create a unique log file name based on the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(step2_errors_folder_path, f"{database_name}_step2_errors_{timestamp}.txt")
        validation_file_path2 = os.path.join(validation_folder_path2, f"{database_name}_Validation_step2_report_{timestamp}.txt")

        try:
            start_time = time.time()

            # Loop until the SQL query result is 0
            while True:
                process = subprocess.Popen(
                    ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                     "-serverName", server_name, "-databaseName", database_name, "-logFolderPath", step2_errors_folder_path],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )

                stdout, stderr = process.communicate()
                stderr = stderr.decode("utf-8")

                if stderr:
                    with open(log_file_path, 'a') as log_file:
                        log_file.write("\nPowerShell Errors:\n")
                        log_file.write(stderr)
                    end_time = time.time()
                    time_taken = end_time - start_time
                    minutes = int(time_taken // 60)
                    seconds = int(time_taken % 60)
                    milliseconds = int((time_taken * 1000) % 1000)

                    return jsonify({
                        "status": "Error running PowerShell",
                        "log_file": log_file_path,
                        "stderr": stderr,
                        "time_taken": time_taken,
                        "minutes": minutes,
                        "seconds": seconds,
                        "milliseconds": milliseconds
                    })

                conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()

                cursor.execute("SELECT count(*) FROM sys.objects WHERE schema_id = schema_id('lmn')")
                result = cursor.fetchone()[0]

                with open(validation_file_path2, 'a') as validation_file:
                    validation_file.write(f"SQL Query Result: {result}\n")

                    if result == 0:
                        validation_file.write("No objects found with 'testowner' schema.\n")
                        end_time = time.time()
                        time_taken = end_time - start_time
                        minutes = int(time_taken // 60)
                        seconds = int(time_taken % 60)
                        milliseconds = int((time_taken * 1000) % 1000)

                        return jsonify({
                            "status": "PowerShell script executed successfully. No errors in the schema.",
                            "validation_file": validation_file_path2,
                            "time_taken": time_taken,
                            "minutes": minutes,
                            "seconds": seconds,
                            "milliseconds": milliseconds
                        })
                    else:
                        validation_file.write("Schema issue detected.\n")

                if result == 0:
                    break
                else:
                    print("Schema issue still exists. Retrying...")

            end_time = time.time()
            time_taken = end_time - start_time
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)

            return jsonify({
                "status": "PowerShell script executed successfully.",
                "validation_file": validation_file_path2,
                "time_taken": time_taken,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": milliseconds
            })

        except Exception as e:
            with open(log_file_path, 'a') as log_file:
                log_file.write("\nException:\n")
                log_file.write(str(e))
            return jsonify({"error": f"An error occurred: {str(e)}"})

        finally:
            end_time = time.time()
            time_taken = end_time - start_time
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Total Time taken: {minutes} minutes")
            
    return jsonify({"error": "Missing parameters: server, database, or folder path"}), 400







@app.route('/run_powershell3', methods=['GET'])
def run_powershell3():
    database_name = request.args.get('database')
    folder_path = request.args.get('folder_path')  # Get folder path from query string
    if not database_name or not folder_path:
        return jsonify({"error": "Missing parameters: database or folder path"}), 400

    # Connect to SQLite database for session information
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT server_name, step3_errors_folder, validation_folder_path3,schemanamefrom,schemanameto
                 FROM sessions WHERE database_name=? AND folder_created=1''', (database_name,))
    session_data = cursor.fetchone()
    conn.close()

    if session_data:
        server_name, step3_errors_folder_path, validation_folder_path3,schemanamefrom,schemanameto = session_data
    else:
        return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400
    print("started")
    powershell_script_path = r".\scripts\step3.ps1"
    if server_name and database_name and step3_errors_folder_path:
        # Create a unique log file name based on the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(step3_errors_folder_path, f"{database_name}_step3_errors_{timestamp}.txt")
        validation_folder_path3 = os.path.join(validation_folder_path3, f"{database_name}_Validation_step3_report_{timestamp}.txt")

        try:
            # Start the timer
            start_time = time.time()

            # Running the PowerShell script via subprocess (passing the script file path)
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                 "-serverName", server_name, "-databaseName", database_name,"-Fromsch",schemanamefrom,"-Tosch",schemanameto, "-logFolderPath", step3_errors_folder_path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            # Communicating with the process to capture stdout and stderr
            stdout, stderr = process.communicate()

            # Decoding the byte output to string
            stdout = stdout.decode("utf-8")
            stderr = stderr.decode("utf-8")

            # Write PowerShell output and errors to the log file
            if stderr:
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nPowerShell Errors:\n")
                    log_file.write(stderr)

                end_time = time.time()
                time_taken = end_time - start_time
                minutes = int(time_taken // 60)
                seconds = int(time_taken % 60)
                milliseconds = int((time_taken * 1000) % 1000)

                return jsonify({
                    "status": "PowerShell script executed with errors.",
                    "log_file": log_file_path,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })

            # Now that the PowerShell script has run without errors, connect to SQL Server to execute the ALTER SCHEMA command
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            print("starting sql")
            # Concatenate all ALTER SCHEMA commands for user-defined types in 'corpuser' schema
            sql_query = """
                        DECLARE @typeName SYSNAME;
                        DECLARE @sql NVARCHAR(MAX);
                        
                        DECLARE type_cursor CURSOR FOR
                        SELECT name
                        FROM sys.types
                        WHERE is_user_defined = 1
                        AND schema_id = SCHEMA_ID('corpuser');
                        
                        OPEN type_cursor;
                        
                        FETCH NEXT FROM type_cursor INTO @typeName;
                        
                        WHILE @@FETCH_STATUS = 0
                        BEGIN
                            BEGIN TRY
                                SET @sql = N'ALTER SCHEMA testowner TRANSFER TYPE::[corpuser].[' + QUOTENAME(@typeName) + N']';
                                PRINT 'Executing: ' + @sql;
                                EXEC sp_executesql @sql;
                            END TRY
                            BEGIN CATCH
                                PRINT 'Failed to transfer type: [corpuser].[' + @typeName + ']';
                                PRINT 'Error: ' + ERROR_MESSAGE();
                            END CATCH;
                        
                            FETCH NEXT FROM type_cursor INTO @typeName;
                        END
                        
                        CLOSE type_cursor;
                        DEALLOCATE type_cursor;
                        """

            # Execute the ALTER SCHEMA SQL query
            cursor.execute(sql_query)
            cursor.commit()
            cursor.close()

            # Validation: Check if the schema transfer was successful
            cursor = conn.cursor()
            cursor.execute("SELECT count(*) FROM sys.types WHERE schema_id = SCHEMA_ID('testowner') AND is_user_defined = 1")
            result = cursor.fetchone()[0]
            cursor.close()

            with open(validation_folder_path3, 'a') as validation_file:
                if result > 0:
                    validation_file.write(f"SQL Query Result: {result} \n")
                    validation_file.write(f"User-defined types successfully transferred to 'testowner' schema.\n")
                else:
                    validation_file.write(f"SQL Query Result: {result} \n")
                    validation_file.write(f"Failed to transfer user-defined types.\n")

            # End the timer and calculate time taken
            end_time = time.time()
            time_taken = end_time - start_time
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print("end")
            return jsonify({
                "status": "PowerShell script executed successfully, SQL schema transfer completed.",
                "validation_file": validation_folder_path3,
                "time_taken": time_taken,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": milliseconds
            })

        except Exception as e:
            # Log any exception that occurs during the process
            with open(log_file_path, 'a') as log_file:
                log_file.write("\nException:\n")
                log_file.write(str(e))
            return jsonify({"error": f"An error occurred: {str(e)}"})

    return jsonify({"error": "Missing parameters: server, database, or folder path"}), 400




@app.route('/run_powershell4', methods=['GET'])
def run_powershell4():
    database_name = request.args.get('database')
    
    # Retrieve session data
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT server_name, step4_errors_folder, validation_folder_path4,schemanamefrom,schemanameto
                 FROM sessions WHERE database_name=? AND folder_created=1''',
              (database_name,))
    session_data = cursor.fetchone()
    conn.close()
    print('started1')
    if session_data:
        server_name, step4_errors_folder_path, validation_folder_path4,schemanamefrom,schemanameto = session_data
        print(server_name, step4_errors_folder_path, validation_folder_path4,schemanamefrom,schemanameto)
    else:
        return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    powershell_script_path = r".\scripts\step4.ps1"
    print('started2')
    if server_name and database_name and step4_errors_folder_path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(step4_errors_folder_path, f"{database_name}_step4_errors_{timestamp}.txt")
        validation_file_path = os.path.join(validation_folder_path4, f"{database_name}_Validation_4_Testowner_report_{timestamp}.txt")

        try:
            start_time = time.time()

            # Running PowerShell script via subprocess
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                 "-serverName", server_name, "-databaseName", database_name,"-Tosch",schemanameto,"-Fromsch",schemanamefrom, "-logFolderPath", step4_errors_folder_path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            # Capture output
            stdout, stderr = process.communicate()
            stdout = stdout.decode("utf-8")
            stderr = stderr.decode("utf-8")

            if stderr:
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nPowerShell Errors:\n")
                    log_file.write(stderr)

                end_time = time.time()
                time_taken = end_time - start_time
                minutes = int(time_taken // 60)
                seconds = int(time_taken % 60)
                milliseconds = int((time_taken * 1000) % 1000)

                return jsonify({
                    "status": "PowerShell script executed with errors.",
                    "log_file": log_file_path,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })

            # No errors in PowerShell, proceed with validation
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            cursor.execute("select *  from sys.objects where schema_name(schema_id) = 'corpuser' and parent_object_id <> 0")
            result = cursor.fetchone()

            # Open validation file and log results
            with open(validation_file_path, 'a') as validation_file:
                if result is None:
                    validation_file.write(f"SQL Query Result: {result} \n")
                    validation_file.write(f"tablename found from objects with testowner schema.\n")
                    end_time = time.time()
                    time_taken = end_time - start_time
                    minutes = int(time_taken // 60)
                    seconds = int(time_taken % 60)
                    milliseconds = int((time_taken * 1000) % 1000)

                    return jsonify({
                        "status": "PowerShell script executed successfully. No errors in the schema.",
                        "validation_file": validation_file_path,
                        "time_taken": time_taken,
                        "minutes": minutes,
                        "seconds": seconds,
                        "milliseconds": milliseconds
                    })
                else:
                    validation_file.write(f"SQL Query Result: {result} \n")
                    validation_file.write(f"Schema issue detected.\n")
                    end_time = time.time()
                    time_taken = end_time - start_time
                    minutes = int(time_taken // 60)
                    seconds = int(time_taken % 60)
                    milliseconds = int((time_taken * 1000) % 1000)

                    return jsonify({
                        "status": f"Still there are {result} objects in corpuser.",
                        "log_file": log_file_path,
                        "time_taken": time_taken,
                        "minutes": minutes,
                        "seconds": seconds,
                        "milliseconds": milliseconds
                    })

        except Exception as e:
            with open(log_file_path, 'a') as log_file:
                log_file.write("\nException:\n")
                log_file.write(str(e))

    return jsonify({"error": f"An error occurred: "})

@app.route('/run_powershell5', methods=['GET'])
def run_powershell5():
    print("Session Variables:", session)
    # Retrieve parameters from the query string or session
    #server_name = request.args.get('server')
    database_name = request.args.get('database')
    #db_errors_folder_path = session.get('db_errors_folder')
    #scripts_folder_path = session.get('scripts_folder_path')
    #validation_folder_path4 = session.get('validation_folder_path4')

    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT server_name,step5_errors_folder,scripts_folder_path, validation_folder_path5 
                 FROM sessions WHERE database_name=? AND folder_created=1''',
              (database_name,))
    session_data = cursor.fetchone()
    conn.close()

    if session_data:
        server_name, step5_errors_folder_path, scripts_folder_path, validation_folder_path5 = session_data
        print(server_name, step5_errors_folder_path, scripts_folder_path, validation_folder_path5)
    else:
        return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    if not server_name or not database_name:
        return jsonify({"error": "Missing required parameters: server or database"}), 400

    # Path to the PowerShell scripts
    powershell_script_path = r".\scripts\step5.ps1"
    powershell_script_path_b = r".\scripts\step5b.ps1"
    
    # Create a unique timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(step5_errors_folder_path, f"{database_name}_step5_errors_{timestamp}.txt")
    validation_file_path = os.path.join(validation_folder_path5, f"{database_name}_Validation_5_Testowner_report_{timestamp}.txt")
    
    try:
        start_time = time.time()
        # Running the first PowerShell script (step5.ps1)
        process = subprocess.Popen(
            ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
             "-serverName", server_name, "-databaseName", database_name, "-outputRootFolder", scripts_folder_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Wait for the process to complete and capture its output
        stdout, stderr = process.communicate()

        # Decode stdout and stderr
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")
        print(stdout)
        
        # If there are errors, log them in a file
        if stderr:
            with open(log_file_path, 'a') as log_file:
                log_file.write("\nPowerShell Errors (step5.ps1):\n")
                log_file.write(stderr)

            end_time = time.time()
            time_taken = end_time - start_time  # Time in seconds

            # Convert time_taken into minutes, seconds, and milliseconds
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Time taken: {time_taken:.2f} minutes")

            return jsonify({
                "status": "PowerShell script step5.ps1 executed with errors.",
                "log_file": log_file_path,
                "time_taken": time_taken,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": milliseconds
            })

        # Extract total files created from stdout (for step5.ps1)
        total_files_created = None
        for line in stdout.splitlines():
            if "Total files created" in line:
                total_files_created = int(line.split(":")[1].strip())  # Assuming the value is an integer

        if total_files_created is None:
            total_files_created = 0

        # If step5.ps1 succeeded, now run step5b.ps1
        process_b = subprocess.Popen(
            ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path_b,
             "-serverName", server_name, "-databaseName", database_name, "-outputFolder", scripts_folder_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Wait for the process to complete and capture its output
        stdout_b, stderr_b = process_b.communicate()

        # Decode stdout and stderr for step5b.ps1
        stdout_b = stdout_b.decode("utf-8")
        stderr_b = stderr_b.decode("utf-8")
        print(stdout_b)
        
        # Log errors from step5b.ps1
        if stderr_b:
            with open(log_file_path, 'a') as log_file:
                log_file.write("\nPowerShell Errors (step5b.ps1):\n")
                log_file.write(stderr_b)

            end_time = time.time()
            time_taken = end_time - start_time  # Time in seconds

            # Convert time_taken into minutes, seconds, and milliseconds
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Time taken: {time_taken:.2f} minutes")

            return jsonify({
                "status": "PowerShell script step5b.ps1 executed with errors.",
                "log_file": log_file_path,
                "time_taken": time_taken,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": milliseconds
            })

        # If both scripts succeed, write validation and counts
        # Connect to the database and get the count of objects
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        # SQL query to count the objects
        cursor.execute("select count(*) from sys.objects where type in ('P', 'V', 'FN','FS','FT','TR')")
        result = cursor.fetchone()
        object_count = result[0] if result else 0

        # Open the validation file for writing
        with open(validation_file_path, 'a') as validation_file:
            # Write the SQL query result and the total files created
            validation_file.write(f"SQL Query Result: {object_count} \n")
            validation_file.write(f"Total Files Created (from PowerShell step5): {total_files_created} \n")

            # Compare the counts
            if object_count == total_files_created:
                validation_file.write("Validation Successful: Counts match.\n")
            else:
                validation_file.write("Validation Failed: Mismatch in counts.\n")

        end_time = time.time()
        time_taken = end_time - start_time  # Time in seconds

        # Convert time_taken into minutes, seconds, and milliseconds
        minutes = int(time_taken // 60)
        seconds = int(time_taken % 60)
        milliseconds = int((time_taken * 1000) % 1000)
        print(f"Time taken: {time_taken:.2f} minutes")

        return jsonify({
            "status": "Both PowerShell scripts executed successfully.",
            "validation_file": validation_file_path,
            "time_taken": time_taken,
            "minutes": minutes,
            "seconds": seconds,
            "milliseconds": milliseconds
        })

    except Exception as e:
        # Handle any unexpected errors and log them
        with open(log_file_path, 'a') as log_file:
            log_file.write("\nException:\n")
            log_file.write(str(e))
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    finally:
        # Make sure to calculate time even if there’s an error or early return
        end_time = time.time()
        time_taken = end_time - start_time  # Time in seconds

        # Convert time_taken into minutes, seconds, and milliseconds
        minutes = int(time_taken // 60)
        seconds = int(time_taken % 60)
        milliseconds = int((time_taken * 1000) % 1000)
        print(f"Time taken: {time_taken:.2f} minutes")

import os
import shutil
import re

def check_and_move_cross_db_files(analyze_path, destination_path, requested_db):
    cross_db_pattern = re.compile(r"([a-zA-Z_][a-zA-Z0-9_]*)\.[a-zA-Z_][a-zA-Z0-9_]*\.", re.IGNORECASE)
    cross_db_files = {}

    encodings_to_try = ["utf-8", "latin1", "cp1252", "utf-16"]

    for foldername, _, filenames in os.walk(analyze_path):
        for filename in filenames:
            if filename.endswith(".sql"):
                file_path = os.path.join(foldername, filename)
                content = None

                for encoding in encodings_to_try:
                    try:
                        # Try to open the file with the current encoding
                        with open(file_path, "r", encoding=encoding) as sql_file:
                            content = sql_file.read()
                        break  # If we successfully read the file, break out of the loop
                    except UnicodeDecodeError:
                        print(f"Error reading {file_path}: UnicodeDecodeError with {encoding} - Trying next encoding.")
                    except Exception as e:
                        print(f"Error reading {file_path} with {encoding}: {e}")
                        break  # Stop trying further encodings if there's another exception

                # Skip the file if content couldn't be read with any encoding
                if content is None:
                    print(f"Skipping {file_path} due to reading errors.")
                    continue

                # Find all database references in the file
                matches = cross_db_pattern.findall(content)
                unique_dbs = set(matches)  # Remove duplicates

                # Identify cross-database references
                cross_dbs = [db for db in unique_dbs if db.lower() != requested_db.lower()]
                if cross_dbs:
                    cross_db_files[file_path] = cross_dbs

    # Copy files to respective cross-db folders
    for file_path, dbs in cross_db_files.items():
        for cross_db in dbs:
            # Generate the relative path and new destination path
            relative_path = os.path.relpath(file_path, analyze_path)
            new_path = os.path.join(destination_path, "crossdb", cross_db, relative_path)

            # Ensure the destination directory exists
            destination_dir = os.path.dirname(new_path)
            if not os.path.exists(destination_dir):
                print(f"Creating destination directory: {destination_dir}")
                os.makedirs(destination_dir, exist_ok=True)  # Create necessary directories

            # Try to copy the file and handle possible errors
            try:
                print(f"Copying {file_path} -> {new_path}")
                shutil.copy2(file_path, new_path)  # Copy the file (preserves metadata like timestamps)
            except FileNotFoundError as e:
                print(f"Error copying file {file_path}: {e}")
            except Exception as e:
                print(f"Unexpected error while copying {file_path}: {e}")

    return cross_db_files

def add_schema_to_sql_objects(folder_path, schema_name):
    try:
        # Iterate through all files in the folder
        for filename in os.listdir(folder_path):
            if filename.endswith(".sql") and not sql_file.lower().startswith('dbo'):
                sql_file_path = os.path.join(folder_path, filename)
                
                with open(sql_file_path, 'r') as file:
                    # Read all lines from the file
                    lines = file.readlines()

                # Patterns to match SQL object creation and alteration statements
                patterns = {
                    "CREATE PROCEDURE": r'(?i)(CREATE\s+PROCEDURE\s+)(?!\[\w+\]\.|\w+\.)(\[?\w+\]?)',
                    "ALTER PROCEDURE": r'(?i)(ALTER\s+PROCEDURE\s+)(?!\[\w+\]\.|\w+\.)(\[?\w+\]?)',
                    "CREATE VIEW": r'(?i)(CREATE\s+VIEW\s+)(?!\[\w+\]\.|\w+\.)(\[?\w+\]?)',
                    "ALTER VIEW": r'(?i)(ALTER\s+VIEW\s+)(?!\[\w+\]\.|\w+\.)(\[?\w+\]?)',
                    "CREATE FUNCTION": r'(?i)(CREATE\s+FUNCTION\s+)(?!\[\w+\]\.|\w+\.)(\[?\w+\]?)',
                    "ALTER FUNCTION": r'(?i)(ALTER\s+FUNCTION\s+)(?!\[\w+\]\.|\w+\.)(\[?\w+\]?)',
                    "CREATE TRIGGER": r'(?i)(CREATE\s+TRIGGER\s+)(?!\[\w+\]\.|\w+\.)(\[?\w+\]?)',
                    "ALTER TRIGGER": r'(?i)(ALTER\s+TRIGGER\s+)(?!\[\w+\]\.|\w+\.)(\[?\w+\]?)'
                }

                # Process only the first 8 lines to check and update SQL object definitions
                for i in range(min(8, len(lines))):
                    for object_type, pattern in patterns.items():
                        if re.search(pattern, lines[i]):
                            lines[i] = re.sub(
                                pattern,
                                lambda match: f"{match.group(1)}{schema_name}.{match.group(2)}",
                                lines[i]
                            )

                # Write the modified lines back to the same file
                with open(sql_file_path, 'w') as file:
                    file.writelines(lines)

                print(f"Schema name added where necessary in the first 8 lines of file: {sql_file_path}")

    except FileNotFoundError:
        print(f"Error: Folder not found at path {folder_path}")
    except Exception as e:
        print(f"An error occurred: {e}")


def add_schema_to_sql_objects1(folder_path, schema):
    """
    This function will add the schema name to the SQL objects in all the SQL files in the specified folder.
    It will modify the SQL code to include the schema (e.g., [schema].[object_name]) and ensure proper replacements.
    """
    # Collect all the SQL files in the folder and subfolders
    sql_files = []
    for folder in os.listdir(folder_path):
        folder_full_path = os.path.join(folder_path, folder)
        if os.path.isdir(folder_full_path):
            # Loop through the subfolder to find .sql files
            for sql_file in os.listdir(folder_full_path):
                # Skip files that start with "dbo"
                if sql_file.endswith('.sql') and not sql_file.lower().startswith('dbo'):
                    sql_files.append(os.path.join(folder_full_path, sql_file))

    # If no SQL files are found, raise an error
    if not sql_files:
        raise ValueError(f"No SQL files found in the specified directory: {folder_path}")

    # Process each SQL file
    validation_errors = []
    for sql_file in sql_files:
        file_error = False
        try:
            with open(sql_file, 'r', encoding='utf-8', errors='ignore') as file:
                sql_content = file.read()

            # Modify SQL content:
            # 1. Add schema name to 'ALTER' and 'CREATE' for procedures, views, functions, and triggers
            sql_content = re.sub(r'(?i)\bALTER\s+(PROCEDURE|VIEW|FUNCTION|TRIGGER|PROC)\s+(?!\[\w+\]\.)', 
                                 lambda m: f'ALTER {m.group(1)} {schema}.', sql_content)
            sql_content = re.sub(r'(?i)\bCREATE\s+(PROCEDURE|VIEW|FUNCTION|TRIGGER|PROC)\s+(?!\[\w+\]\.)', 
                                 lambda m: f'CREATE {m.group(1)} {schema}.', sql_content)

            # 2. Ensure all table references have the schema name (e.g., [testowner].BGTTrnExchangeRates), skipping if already prefixed
            sql_content = re.sub(r'(?i)(\bFROM\s+|\bINTO\s+|\bUPDATE\s+|\bSELECT\s+|\bDELETE\s+)\s*(?!\[\w+\]\.)\S+', 
                                 lambda m: f'{m.group(1)}{schema}.{m.group(2)}' if m.lastgroup else m.group(0), sql_content)

            # 3. Replace occurrences of 'corpuser' with the schema name
            sql_content = sql_content.replace('corpuser', schema)

            # 4. Ensure 'create' statements are replaced with 'alter', skipping if already replaced
            sql_content = re.sub(r'(?i)\bCREATE\s+(PROCEDURE|VIEW|FUNCTION|TRIGGER|PROC)\s+', 
                                 lambda m: f'ALTER {m.group(1)} ' if m.group(1) else '', sql_content)

            # Write the modified content back to the file
            with open(sql_file, 'w', encoding='utf-8') as file:
                file.write(sql_content)

            # Validation: Check if modifications were successful
            for line in sql_content.splitlines():
                # Check for ALTER/CREATE without schema
                if re.search(r'(?i)(ALTER|CREATE)\s+(PROCEDURE|VIEW|FUNCTION|TRIGGER|PROC)\s+[^\[]*\w+\.[^\]]+', line):
                    if schema.lower() not in line.lower():
                        validation_errors.append(f"Error: Schema '{schema}' not added in {sql_file}")
                        file_error = True
                        break  # Stop checking further if an error is found

                # Check if 'corpuser' was replaced with the schema name
                if 'corpuser' in line:
                    validation_errors.append(f"Error: 'corpuser' not replaced with '{schema}' in {sql_file}")
                    file_error = True
                    break  # Stop checking further if an error is found

                # Ensure 'create' statements were replaced with 'alter'
                if any(keyword in line for keyword in ['create proc', 'create procedure', 'create view', 'create function', 'create trigger']):
                    validation_errors.append(f"Error: 'create' statements not replaced with 'alter' in {sql_file}")
                    file_error = True
                    break  # Stop checking further if an error is found

            if file_error:
                validation_errors.append(f"File: {sql_file} - ERROR\n")
            else:
                validation_errors.append(f"File: {sql_file} - Successfully modified\n")
        except Exception as e:
            validation_errors.append(f"Error processing file {sql_file}: {str(e)}")

    # Return any validation errors that were found
    return validation_errors


@app.route('/run_powershell6', methods=['GET'])
def run_powershell6():
    #server_name = request.args.get('server')
    database_name = request.args.get('database')
    #db_errors_folder_path = session.get('db_errors_folder')
    #scripts_folder_path = session.get('scripts_folder_path')
    #validation_folder_path5 = session.get('validation_folder_path5')
    
    # Check if folder has been created
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT server_name,step6_errors_folder,folder_path, scripts_folder_path, validation_folder_path6,schemanamefrom,schemanameto 
                 FROM sessions WHERE  database_name=? AND folder_created=1''',
              (database_name,))
    session_data = cursor.fetchone()
    conn.close()
    #scripts_folder_path = scripts_folder_path
    print("sql")
    if session_data:
        server_name,step6_errors_folder_path,folder_path,scripts_folder_path, validation_folder_path6,schemanamefrom,schemanameto = session_data
        print(server_name,step6_errors_folder_path,scripts_folder_path,folder_path, validation_folder_path6)
        print("-------------------------------------------------------------------------------------------")
        print(schemanamefrom,schemanameto)
    else:
        return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    powershell_script_path = r".\scripts\step6.ps1"
    
    if server_name and database_name and step6_errors_folder_path:
        # Create a unique timestamp for the log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Declare log file paths
        log_file_path = os.path.join(step6_errors_folder_path, f"{database_name}_step6_errors_{timestamp}.txt")
        validation_file_path = os.path.join(validation_folder_path6, f"{database_name}_Validation_6_Testowner_report_{timestamp}.txt")
        
        try:
            start_time = time.time()
            print("powershell execution started")
            # Running the PowerShell script via subprocess
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                 "-rootFolderPath", scripts_folder_path,"-Fromsch",schemanamefrom,"-Tosch",schemanameto],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            print("half execution")

            # Wait for the process to complete and capture its output
            stdout, stderr = process.communicate()
            print ("quATAR execution")
            # Decode stdout and stderr
            stdout = stdout.decode("utf-8")
            stderr = stderr.decode("utf-8")
            print("powershell executed")
            # Check if any errors were produced during the execution of the PowerShell script
            if stderr:
                # Log the error in the file only if the process encountered an issue
                with open(log_file_path, 'a',errors='ignore') as log_file:
                    log_file.write("\nPowerShell Errors:\n")
                    log_file.write(stderr)
                    end_time = time.time()
                    time_taken = end_time - start_time  # Time in seconds

                    # Convert time_taken into minutes, seconds, and milliseconds
                    minutes = int(time_taken // 60)
                    seconds = int(time_taken % 60)
                    milliseconds = int((time_taken * 1000) % 1000)
                    print(f"Time taken: {time_taken:.2f} minutes")

                return jsonify({
                    "status": "PowerShell script executed with errors.",
                    "log_file": log_file_path,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })

            # If no errors from PowerShell, check if the files have been modified as expected
            # Check if the folder paths exist and contain SQL files
            sql_files = []
            for folder_path in os.listdir(scripts_folder_path):
                folder_full_path = os.path.join(scripts_folder_path, folder_path)
                if os.path.isdir(folder_full_path):
                    # Loop through the subfolder to find .sql files
                    for sql_file in os.listdir(folder_full_path):
                        # Skip files that start with "dbo"
                        if sql_file.endswith('.sql') and not sql_file.lower().startswith('dbo'):
                            sql_files.append(os.path.join(folder_full_path, sql_file))

            # If no SQL files are found, raise an error
            if not sql_files:
                raise ValueError(f"No SQL files found in the specified directory: {scripts_folder_path}")            

            folder = scripts_folder_path  # Using scripts folder path
            schema = "[testowner]"  # Replace with your desired schema name

            #add_schema_to_sql_objects(folder, schema)
            add_schema_to_sql_objects1(folder, schema)
            # Validation errors for SQL files

            # Validation errors for SQL files
            validation_errors = []  # To store any validation errors for SQL files
            for sql_file in sql_files:
                file_path = os.path.join(scripts_folder_path, sql_file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                        lines = [file.readline().strip() for _ in range(8)]
                except UnicodeDecodeError:
                    # If 'utf-8' fails, try 'windows-1252' encoding
                    with open(file_path, 'r', encoding='windows-1252') as file:
                        lines = [file.readline().strip() for _ in range(8)]

                #with open(file_path, 'r') as file:
                    #lines = [file.readline().strip() for _ in range(8)]  # Read only the first 8 lines

                    # Check if the content was modified correctly
                    file_error = False  # Flag to track if there's an error for this file

                    # Check if schema was added correctly to the object names
                    for line in lines:
                        if re.search(r'(?i)(CREATE|ALTER)\s+(PROCEDURE|VIEW|FUNCTION|TRIGGER)\s+[^\[]*\w+\.[^\]]+', line):
                            # Check if the schema is added (i.e., it should look like schema_name.object_name)
                            if schema.lower() not in line.lower():
                                validation_errors.append(f"Error: Schema '{schema}' not added in {sql_file}")
                                file_error = True
                                break  # Stop checking further if an error is found

                    # Check that 'corpuser' was replaced with 'testowner'
                    for line in lines:
                        if 'corpuser' in line:
                            validation_errors.append(f"Error: 'corpuser' not replaced in {sql_file}")
                            file_error = True
                            break  # Stop checking further if an error is found

                    # Check if 'create' statements were replaced with 'alter'
                    for line in lines:
                        if any(keyword in line for keyword in ['create proc', 'create procedure', 'create view', 'create function', 'create trigger']):
                            validation_errors.append(f"Error: 'create' statements not replaced with 'alter' in {sql_file}")
                            file_error = True
                            break  # Stop checking further if an error is found

                    # If there's an error with this file, mark it
                    if file_error:
                        validation_errors.append(f"File: {sql_file} - ERROR\n")
                    else:
                        validation_errors.append(f"File: {sql_file} - Successfully modified\n")

            # Write the validation report
            with open(validation_file_path, 'a') as validation_file:
                if validation_errors:
                    validation_file.write("Validation failed for the following files:\n")
                    for error in validation_errors:
                        validation_file.write(f"{error}\n")
                else:
                    validation_file.write("Validation successful. All SQL files were modified correctly.\n")
            
            end_time = time.time()
            time_taken = end_time - start_time  # Time in seconds

                    # Convert time_taken into minutes, seconds, and milliseconds
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)

            print(f"Time taken: {time_taken:.2f} minutes")

            return jsonify({
                            "status": "PowerShell script executed and validated.",
                            "validation_file": validation_file_path,
                            "time_taken": time_taken,
                            "minutes": minutes,
                            "seconds": seconds,
                            "milliseconds": milliseconds
            })            

        except Exception as e:
            # Handle any unexpected errors that occurred during the execution
            with open(log_file_path, 'a') as log_file:
                log_file.write("\nException:\n")
                log_file.write(str(e))
            return jsonify({"error": f"An error occurred: {str(e)}"})
        finally:
    # Make sure to calculate time even if there’s an error or early return
            end_time = time.time()
            time_taken = end_time - start_time  # Time in seconds

                    # Convert time_taken into minutes, seconds, and milliseconds
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Time taken: {time_taken:.2f} minutes")

    return jsonify({"error": "Missing parameters: server, database, or folder path"}), 400



@app.route('/run_powershell7', methods=['GET'])
def run_powershell7():
    # Retrieve parameters from the query string or session
    database_name = request.args.get('database')

    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT server_name,step7_errors_folder,scripts_folder_path,folder_path, validation_folder_path7 
                 FROM sessions WHERE  database_name=? AND folder_created=1''',
              (database_name,))
    session_data = cursor.fetchone()
    conn.close()

    if session_data:
        server_name, step7_errors_folder_path, scripts_folder_path, folder_path, validation_folder_path7 = session_data
        print(server_name, step7_errors_folder_path, scripts_folder_path, folder_path, validation_folder_path7)
    else:
        return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400
    print("Processing database:", database_name)
    
    cross_db_results = check_and_move_cross_db_files(scripts_folder_path, folder_path, database_name)
    
    # Path to the PowerShell script
    powershell_script_path = r".\scripts\step7.ps1"
    
    # Create a unique timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(step7_errors_folder_path, f"{database_name}_ErrorLog_db_errors_{timestamp}.txt")
    validation_file_path = os.path.join(validation_folder_path7, f"{database_name}_Validation_7_Testowner_report_{timestamp}.txt")

    total_time_taken = 0  # Track total time taken for all attempts

    try:
        # Run PowerShell script 3 times regardless of success or failure
        for attempt in range(3):  # Try exactly 3 times
            print(f"Attempt {attempt + 1} of 3 to run PowerShell script.")

            start_time = time.time()
            # Running the PowerShell script via subprocess
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                 "-serverName", server_name, "-databaseName", database_name, "-scriptParentFolder", scripts_folder_path,"-logFolder",step7_errors_folder_path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            # Wait for the process to complete and capture its output
            stdout, stderr = process.communicate()

            # Decode stdout and stderr
            stdout = stdout.decode("utf-8")
            stderr = stderr.decode("utf-8")

            # Log output for the current attempt
            with open(log_file_path, 'a') as log_file:
                log_file.write(f"\nAttempt {attempt + 1} Output:\n")
                log_file.write(f"stdout:\n{stdout}\nstderr:\n{stderr}\n")

            # Time calculation for the current attempt
            end_time = time.time()
            time_taken = end_time - start_time  # Time in seconds
            total_time_taken += time_taken  # Add to total time taken

            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Time taken for attempt {attempt + 1}: {minutes} minutes {seconds} seconds {milliseconds} ms")

        # After 3 attempts, proceed to validation
        total_files_created = None
        match = re.search(r"Total files created\s*[:\-]?\s*(\d+)", stdout)
        if match:
            total_files_created = int(match.group(1))  # Extract the number from the match
        else:
            total_files_created = 0  # Default value if no match is found

        print(f"Total Files Created: {total_files_created}")

        # Count files in all subfolders
        def count_files_in_subfolders(base_folder):
            total_files = 0
            for dirpath, _, filenames in os.walk(base_folder):
                total_files += len(filenames)
            return total_files

        total_files_in_subfolders = count_files_in_subfolders(scripts_folder_path)

        # Connect to the database and get the count of objects
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        cursor.execute("select count(*) from sys.objects where type in ('P', 'V', 'FN','FS','FT','TR')")
        result = cursor.fetchone()
        object_count = result[0] if result else 0

        # Open the validation file for writing
        with open(validation_file_path, 'a') as validation_file:
            validation_file.write(f"SQL Query Result: {object_count} \n")
            validation_file.write(f"Total Files Created (from PowerShell): {total_files_in_subfolders} \n")
            print(object_count, total_files_in_subfolders)

            # Compare the counts
            if object_count == total_files_in_subfolders:
                validation_file.write("Validation Successful: Counts match.\n")
            else:
                validation_file.write("Validation Failed: Mismatch in counts.\n")

        # Time calculation after all attempts
        total_minutes = int((total_time_taken // 60) % 60)
        total_seconds = int(total_time_taken % 60)
        total_milliseconds = int((total_time_taken * 1000) % 1000)
        print(f"Total Time Taken for 3 attempts: {total_minutes} minutes {total_seconds} seconds {total_milliseconds} ms")

        return jsonify({
            "status": "PowerShell script executed 3 times. Validation completed.",
            "validation_file": validation_file_path,
            "total_time_taken": total_time_taken,
            "minutes": total_minutes,
            "seconds": total_seconds,
            "milliseconds": total_milliseconds
        })

    except Exception as e:
        # Handle any unexpected errors and log them
        with open(log_file_path, 'a') as log_file:
            log_file.write("\nException:\n")
            log_file.write(str(e))
        return jsonify({"error": f"An error occurred: {str(e)}"})

    finally:
        # Ensure the total time is calculated regardless of early exits or errors
        total_minutes = int((total_time_taken // 60) % 60)
        total_seconds = int(total_time_taken % 60)
        total_milliseconds = int((total_time_taken * 1000) % 1000)
        print(f"Total Time Taken for all attempts: {total_minutes} minutes {total_seconds} seconds {total_milliseconds} ms")
        return jsonify({
            "status": "PowerShell script executed 3 times. Validation completed.",
            "validation_file": validation_file_path,
            "total_time_taken": total_time_taken,  # This needs to be passed to frontend
            "minutes": total_minutes,
            "seconds": total_seconds,
            "milliseconds": total_milliseconds
        })

        


@app.route('/run_powershell8', methods=['GET'])
def run_powershell8():
    # Get parameters from the request or session
    #server_name = request.args.get('server')
    database_name = request.args.get('database')
    #schema_errors_folder_path = session.get('schema_errors_folder')  # Retrieve the SchemaErrors folder path from session
    #validation_folder_path1 = session.get('validation_folder_path1')
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT server_name,step8_errors_folder,scripts_folder_path,folder_path, validation_folder_path8 
                 FROM sessions WHERE  database_name=? AND folder_created=1''',
              ( database_name,))
    session_data = cursor.fetchone()
    conn.close()

    if session_data:
        server_name,step8_errors_folder_path, scripts_folder_path,folder_path, validation_folder_path8 = session_data
        
        print(server_name,step8_errors_folder_path, scripts_folder_path,folder_path, validation_folder_path8)
    else:
        return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes'
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()


    cursor.execute("SELECT count(*) FROM sys.foreign_keys fk INNER JOIN sys.tables fk_tab ON fk_tab.object_id = fk.parent_object_id INNER JOIN sys.tables pk_tab ON pk_tab.object_id = fk.referenced_object_id CROSS APPLY (SELECT col.[name] + ', ' FROM sys.foreign_key_columns fk_c INNER JOIN sys.columns col ON fk_c.parent_object_id = col.object_id AND fk_c.parent_column_id = col.column_id WHERE fk_c.constraint_object_id = fk.object_id ORDER BY fk_c.constraint_column_id  FOR XML PATH('')) D(fk_columns) CROSS APPLY (SELECT col.[name] + ', ' FROM sys.foreign_key_columns fk_c INNER JOIN sys.columns col ON fk_c.referenced_object_id = col.object_id AND fk_c.referenced_column_id = col.column_id WHERE fk_c.constraint_object_id = fk.object_id ORDER BY fk_c.constraint_column_id FOR XML PATH('')) E(pk_columns)")
    result1 = cursor.fetchone()[0]

    # Check if folder has been created
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        cursor.execute('''SELECT server_name,step8_errors_folder, validation_folder_path8 
                     FROM sessions WHERE  database_name=? AND folder_created=1''',
                  ( database_name,))
        session_data = cursor.fetchone()
        conn.close()

        if session_data:
            server_name,step8_errors_folder_path, validation_folder_path8 = session_data
            print(server_name,step8_errors_folder_path, validation_folder_path8)
        else:
            return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    except sqlite3.Error as e:
        # Catch database-specific errors
        print(f"Database error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        # Catch other general errors
        print(f"General error: {str(e)}")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    # PowerShell script path
    powershell_script_path = r".\scripts\step8.ps1"
    
    if server_name and database_name and step8_errors_folder_path:
        # Create a unique log file name based on the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(step8_errors_folder_path, f"{database_name}_step8_errors_{timestamp}.txt")
        validation_file_path = os.path.join(validation_folder_path8, f"{database_name}_Validation_step8_report_{timestamp}.txt")

        try:
            start_time = time.time()
            # Running the PowerShell script via subprocess (passing the script file path)
            process = subprocess.Popen(
            ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
            "-serverName", server_name, "-databaseName", database_name, "-logFolderPath", step8_errors_folder_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )


            # Communicating with the process to capture only stderr
            stdout, stderr = process.communicate()

            # Decoding stderr output to string
            stderr = stderr.decode("utf-8")
            stdout = stdout.decode("utf-8")
            print("Running script from: ", powershell_script_path)
            print(stdout)
            # Only log stderr if there is an error
            if stderr:
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nPowerShell Errors:\n")
                    log_file.write(stderr)
                print(server_name)
                print(database_name)
                print(step8_errors_folder_path)

                end_time = time.time()
                time_taken = end_time - start_time  # Time in seconds

                    # Convert time_taken into minutes, seconds, and milliseconds
                minutes = int(time_taken // 60)
                seconds = int(time_taken % 60)
                milliseconds = int((time_taken * 1000) % 1000)
                print(f"Time taken:  {minutes} minutes")

                return jsonify({
                    "status": f"PowerShell script executed with errors.",
                    "log_file": log_file_path,
                    "stderr": stderr,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })
                
            # Now run the SQL query to check if there are any errors in the schema
            conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            
            # Query to count the objects in the schema
            cursor.execute("select count(*) from fk")
            result2 = cursor.fetchone()[0]
            
            with open(validation_file_path, 'a') as validation_file:
                if result1 == result2:
                    validation_file.write(f"SQL Query Result: {result1} and {result2} \n")
                    validation_file.write(f"all Foreign keys inserted into fk table.\n")
                    end_time = time.time()
                    time_taken = end_time - start_time  # Time in seconds

                    # Convert time_taken into minutes, seconds, and milliseconds
                    minutes = int(time_taken // 60)
                    seconds = int(time_taken % 60)
                    milliseconds = int((time_taken * 1000) % 1000)
  
                    print(f"Time taken:  {minutes} minutes")
                    # Return the validation file path
                    return jsonify({
                        "status": "PowerShell script executed successfully. All foreign keys inserted into fk table.",
                        "validation_file": validation_file_path,
                        "time_taken": time_taken,
                        "minutes":minutes,
                        "seconds":seconds,
                        "milliseconds":milliseconds
                    })
                else:
                    validation_file.write(f"SQL Query Result: {result1} and {result2} \n")
                    validation_file.write(f"Count mismatch.Not all Foreignkeys are inserted.\n")
                    
                    end_time = time.time()
                    time_taken = end_time - start_time  # Time in seconds

                    # Convert time_taken into minutes, seconds, and milliseconds
                    minutes = int(time_taken // 60)
                    seconds = int(time_taken % 60)
                    milliseconds = int((time_taken * 1000) % 1000)

                    print(f"Time taken:  {minutes} minutes")
                    return jsonify({
                        "status": f"Count mismatch.Not all Foreignkeys are inserted",
                        "log_file": log_file_path,
                        "time_taken": time_taken,
                        "minutes":minutes,
                        "seconds":seconds,
                        "milliseconds":milliseconds
                    })

        except Exception as e:
            # Handle any exception during the PowerShell execution
            with open(log_file_path, 'a') as log_file:
                log_file.write("\nException:\n")
                log_file.write(str(e))
            return jsonify({"error": f"An error occurred: {str(e)}"})
        finally:
    # Make sure to calculate time even if there’s an error or early return
            end_time = time.time()
            time_taken = end_time - start_time  # Time in seconds

                # Convert time_taken into minutes, seconds, and milliseconds
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)

            print(f"Time taken:  {minutes} minutes")

    return jsonify({"error": "Missing parameters: server, database, or folder path"}), 400




@app.route('/run_powershell9', methods=['GET'])
def run_powershell9():
    # Get parameters from the request
    database_name = request.args.get('database')
    
    # Check if the database_name is provided
    if not database_name:
        return jsonify({"error": "Missing database parameter"}), 400
    
    # Connect to SQLite database for session information
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        cursor.execute('''SELECT server_name, step9_errors_folder, validation_folder_path9 
                     FROM sessions WHERE database_name=? AND folder_created=1''',
                  (database_name,))
        session_data = cursor.fetchone()
        conn.close()

        if session_data:
            server_name, step9_errors_folder_path, validation_folder_path9 = session_data
            print(server_name, step9_errors_folder_path, validation_folder_path9)
        else:
            return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    except sqlite3.Error as e:
        # Catch database-specific errors
        print(f"Database error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        # Catch other general errors
        print(f"General error: {str(e)}")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    # PowerShell script path
    powershell_script_path = r'.\scripts\step9.ps1'
    
    # Check if all required parameters are available
    if server_name and database_name and step9_errors_folder_path:
        # Create a unique log file name based on the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(step9_errors_folder_path, f"{database_name}_step9_errors_{timestamp}.txt")
        validation_file_path = os.path.join(validation_folder_path9, f"{database_name}_Validation_step9_report_{timestamp}.txt")

        # Initialize variables to collect the results for all iterations
        iteration_results = []
        
        # Run the process 3 times
        for run_count in range(3):
            try:
                print(f"Running iteration {run_count + 1} of 3...")
                
                start_time = time.time()
                # Running the PowerShell script via subprocess
                process = subprocess.Popen(
                    ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                     "-serverName", server_name, "-databaseName", database_name, "-logFolderPath", step9_errors_folder_path],
                    stdout=subprocess.PIPE, stderr=subprocess.PIPE
                )

                # Communicating with the process to capture only stderr
                stdout, stderr = process.communicate()

                # Decoding stderr output to string
                stderr = stderr.decode("utf-8")
                stdout = stdout.decode("utf-8")
                print("Running script from: ", powershell_script_path)
                print(stdout)
                
                # Log stderr if there is an error
                if stderr:
                    with open(log_file_path, 'a') as log_file:
                        log_file.write(f"\nPowerShell Errors (Iteration {run_count + 1}):\n")
                        log_file.write(stderr)

                # Now run the SQL query to check if there are any errors in the schema
                conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()

                # Query to count the objects in the schema (example)
                cursor.execute("select count(*) from sys.objects where name = 'fk'")
                result = cursor.fetchone()[0]

                # Writing validation result to file
                with open(validation_file_path, 'a') as validation_file:
                    if result == 0:
                        validation_file.write(f"SQL Query Result: {result}\n")
                        validation_file.write(f"FK table is dropped.\n")
                    else:
                        validation_file.write(f"SQL Query Result: {result} count that shows that fk table is present\n")
                        validation_file.write(f"FK Table is not dropped.\n")

                # Collecting time taken for the current iteration
                end_time = time.time()
                time_taken = end_time - start_time
                minutes = int(time_taken // 60)
                seconds = int(time_taken % 60)
                milliseconds = int((time_taken * 1000) % 1000)

                iteration_results.append({
                    "iteration": run_count + 1,
                    "status": "success" if not stderr else "failure",
                    "log_file": log_file_path,
                    "validation_file": validation_file_path,
                    "stderr": stderr,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })

                print(f"Time taken:  {minutes} minutes {seconds} seconds {milliseconds} milliseconds")

            except Exception as e:
                # Handle any exception during the PowerShell execution
                with open(log_file_path, 'a') as log_file:
                    log_file.write(f"\nException during iteration {run_count + 1}:\n")
                    log_file.write(str(e))
                iteration_results.append({
                    "iteration": run_count + 1,
                    "status": "failure",
                    "error": str(e),
                    "log_file": log_file_path
                })
        end_time = time.time()
        time_taken = end_time - start_time
        minutes = int(time_taken // 60)
        seconds = int(time_taken % 60)
        milliseconds = int((time_taken * 1000) % 1000)
        # Return the results after all 3 iterations are complete
        return jsonify({
            "status": "PowerShell script executed for all iterations.",
            "iterations": iteration_results,
            "time_taken": time_taken,
            "minutes":minutes,
            "seconds":seconds,
            "milliseconds":milliseconds
        })

    return jsonify({"error": "Missing parameters: server, database, or folder path"}), 400

@app.route('/run_powershell10', methods=['GET'])
def run_powershell10():
    # Get parameters from the request or session
    #server_name = request.args.get('server')
    database_name = request.args.get('database')
    # Get optional overrides from the request
    destination_server_input = request.args.get('destination_server')
    destination_database_input = request.args.get('destination_database')
    step10_tablename_files_input = request.args.get('step10_tablename_files')

    print("entered10")
    
    # Check if folder has been created
    try:
        print("Connecting to the SQLite database...")
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        cursor.execute('''SELECT server_name,step10_errors_folder, validation_folder_path10, destination_server, destination_database, step10_tablename_files 
                     FROM sessions WHERE  database_name=? AND folder_created=1''',
                  ( database_name,))
        session_data = cursor.fetchone()
        conn.close()
        print("Database connection closed.")
        print(database_name)
        if session_data:
            server_name,step10_errors_folder_path, validation_folder_path10, destination_server, destination_database, step10_tablename_files = session_data
            destination_server = destination_server_input or destination_server
            destination_database = destination_database_input or destination_database
            step10_tablename_files = step10_tablename_files_input or step10_tablename_files
            print(f"Session data retrieved successfully.{server_name}")
            print(f"step10_errors_folder_path: {step10_errors_folder_path}")
            print(f"validation_folder_path10: {validation_folder_path10}")
            print(f"destination_server: {destination_server}")
            print(f"destination_database: {destination_database}")
            print(f"step10_tablename_files: {step10_tablename_files}")
        else:
            print(f"No session data found for server {server_name} and database {database_name}.")
            return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    except sqlite3.Error as e:
        # Catch database-specific errors
        print(f"Database error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        # Catch other general errors
        print(f"General error: {str(e)}")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    # PowerShell script path
    powershell_script_path = r'.\scripts\step10.ps1'
    
    print(f"PowerShell script path: {powershell_script_path}")

    if server_name and database_name and step10_errors_folder_path:
        # Create a unique log file name based on the current timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(step10_errors_folder_path, f"{database_name}_step10_errors_{timestamp}.txt")
        validation_file_path = os.path.join(validation_folder_path10, f"{database_name}_Validation_step10_report_{timestamp}.txt")

        print(f"Log file path: {log_file_path}")
        print(f"Validation file path: {validation_file_path}")

        try:
            start_time = time.time()
            # Running the PowerShell script via subprocess (passing the script file path)
            print("Running PowerShell script...")
            #server_name,step10_errors_folder_path, validation_folder_path10, destination_server, destination_database, step10_tablename_files = session_data
            print("-------------------------------------------------_____________________")
            print(f"destination_server: {server_name}")
            print(f"destination_database: {database_name}")
            print("-------------------------------------------------_____________________")
            print(f"source server:{destination_server}")
            print(f"source_database: {destination_database}")
            
            

            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                 "-sourceServer", destination_server, "-destinationServer", server_name, "-sourceDatabase", destination_database,
                 "-destinationDatabase", database_name, "-tableNamesFile", step10_tablename_files, "-errorLogFile",log_file_path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            # Communicating with the process to capture only stderr
            stdout, stderr = process.communicate()

            # Decoding stderr output to string
            stderr = stderr.decode("utf-8")
            stdout = stdout.decode("utf-8")
            print("Running script from:", powershell_script_path)
            print("PowerShell Script Output:")
            print(stdout)
            
            # Only log stderr if there is an error
            if stderr:
                print("PowerShell Errors detected.")
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nPowerShell Errors:\n")
                    log_file.write(stderr)
                print(f"Error logged in file: {log_file_path}")

                end_time = time.time()
                time_taken = end_time - start_time
                minutes = int(time_taken // 60)
                seconds = int(time_taken % 60)
                milliseconds = int((time_taken * 1000) % 1000)
                print(f"Time taken for PowerShell execution: {minutes} minutes {seconds} seconds")

                return jsonify({
                    "status": f"PowerShell script executed with errors.",
                    "log_file": log_file_path,
                    "stderr": stderr,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })
            else:
                # Now run the SQL query to check if there are any errors in the schema
                print("Connecting to destination database...")
                conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={destination_server};DATABASE={destination_database};Trusted_Connection=yes;'
                conn = pyodbc.connect(conn_str)
                cursor = conn.cursor()

                # Query to fetch all table names in the destination database
                print("Fetching table names from the destination database...")
                cursor.execute("select name from sys.tables")
                result = cursor.fetchall()  # Fetch all table names

                # Read the table names from the step10_errors_folder_path file
                step10_file_path = step10_tablename_files
                print(f"Reading table names from file: {step10_file_path}")

                try:
                    with open(step10_file_path, 'r') as step10_file:
                        # Assuming each line contains one table name
                        step10_table_names = set(line.strip() for line in step10_file.readlines())
                    print(f"Read table names from file: {len(step10_table_names)} tables.")

                    # Extract the table names from the result
                    db_table_names = set(row[0] for row in result)  # row[0] contains the table name
                    print(f"Fetched table names from the database: {len(db_table_names)} tables.")

                    # Compare the two sets of table names
                    missing_tables = step10_table_names - db_table_names
                    extra_tables = db_table_names - step10_table_names

                    # Write the validation results to the validation file
                    with open(validation_file_path, 'a') as validation_file:
                        if not missing_tables and not extra_tables:
                            validation_file.write("Validation Successful: All table names match.\n")
                        else:
                            if missing_tables:
                                validation_file.write(f"Missing Tables in Database: {', '.join(missing_tables)}\n")
                            if extra_tables:
                                validation_file.write(f"Extra Tables in File: {', '.join(extra_tables)}\n")

                    # Handle validation success or failure
                    if not missing_tables and not extra_tables:
                        validation_status = "Validation successful: All table names match."
                    else:
                        validation_status = "Validation failed: Some tables are missing or extra."
                    print(f"Validation Status: {validation_status}")

                except FileNotFoundError:
                    print(f"File not found: {step10_file_path}")
                    return jsonify({"error": f"File not found: {step10_file_path}"}), 400
                except Exception as e:
                    print(f"Error reading the file: {str(e)}")
                    return jsonify({"error": f"An error occurred while reading the file: {str(e)}"}), 500

                end_time = time.time()
                time_taken = end_time - start_time  # Time in minutes
                minutes = int(time_taken // 60)
                seconds = int(time_taken % 60)
                milliseconds = int((time_taken * 1000) % 1000)
                print(f"Time taken for execution: {minutes} minutes {seconds} seconds")

                return jsonify({
                    "status": validation_status,
                    "validation_file": validation_file_path,
                    "log_file": log_file_path,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })

        except Exception as e:
            # Handle any exception during the PowerShell execution
            print(f"Exception occurred during PowerShell execution: {str(e)}")
            with open(log_file_path, 'a') as log_file:
                log_file.write("\nException:\n")
                log_file.write(str(e))
            return jsonify({"error": f"An error occurred: {str(e)}"})
        finally:
            # Make sure to calculate time even if there’s an error or early return
            end_time = time.time()
            time_taken = end_time - start_time  # Time in minutes
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Final time taken: {minutes} minutes {seconds} seconds")

    return jsonify({"error": "Missing parameters: server, database, or folder path"}), 400

def log_error_to_file(log_file_path, error_message):
    try:
        with open(log_file_path, 'a') as file:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            file.write(f"Error at {timestamp}:\n{error_message}\n\n")
    except Exception as e:
        print(f"Error in log_error_to_file: {str(e)}")


def connect_to_server(server, database):
    """Create a connection to SQL Server using Windows Authentication."""
    connection_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes'
    return pyodbc.connect(connection_str)

def table_exists(cursor, table_name):
    """Check if the table exists in the database."""
    cursor.execute(f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?", table_name.split('.')[1], table_name.split('.')[0])
    return cursor.fetchone()[0] > 0

def list_tables(cursor):
    """List all tables in the database."""
    cursor.execute("SELECT TABLE_SCHEMA, TABLE_NAME FROM INFORMATION_SCHEMA.TABLES")
    tables = cursor.fetchall()
    print("Tables in the source database:")
    for schema, table in tables:
        print(f"{schema}.{table}")

def check_identity_column(cursor, table_name):
    """Check if the table has an identity column."""
    cursor.execute(f"""
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ? AND TABLE_SCHEMA = ?
        AND COLUMNPROPERTY(OBJECT_ID(? + '.' + ?), COLUMN_NAME, 'IsIdentity') = 1
    """, table_name.split('.')[1], table_name.split('.')[0],table_name.split('.')[0],table_name.split('.')[1])
    
    result = cursor.fetchall()
    return [row[0] for row in result]

def move_table(src_conn, dest_conn, table_name):
    """Move table data from the source to the destination."""
    src_cursor = src_conn.cursor()
    dest_cursor = dest_conn.cursor()

    # Check if the table has an identity column
    identity_columns = check_identity_column(dest_cursor, table_name)
    
    if identity_columns:
        # Enable IDENTITY_INSERT for the destination table
        print(f"Enabling IDENTITY_INSERT for {table_name}...")
        dest_cursor.execute(f"SET IDENTITY_INSERT {table_name} ON")
        dest_conn.commit()

    # Copy data from the source table to the destination
    print(f"Copying data from {table_name}...")
    src_cursor.execute(f"SELECT * FROM {table_name}")
    rows = src_cursor.fetchall()
    
    if rows:
        columns = [column[0] for column in src_cursor.description]
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join('?' for _ in columns)})"
        dest_cursor.executemany(insert_query, rows)
        dest_conn.commit()
        print(f"Data from {table_name} moved to the destination server.")
    else:
        print(f"No data found in {table_name} to move.")

    if identity_columns:
        # Disable IDENTITY_INSERT for the destination table
        print(f"Disabling IDENTITY_INSERT for {table_name}...")
        dest_cursor.execute(f"SET IDENTITY_INSERT {table_name} OFF")
        dest_conn.commit()



@app.route('/run_powershell11', methods=['GET'])
def run_powershell11():
    # Get parameters from the request
    server_name = request.args.get('server')
    database_name = request.args.get('database')

    print("Entered /run_powershell11 route")

    try:
        print("Connecting to the SQLite database...")
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        cursor.execute('''SELECT server_name, destination_server, destination_database, step10_tablename_files, step11_errors_folder, validation_folder_path11 
                     FROM sessions WHERE database_name=? AND folder_created=1''',
                  (database_name,))
        session_data = cursor.fetchone()
        conn.close()
        print("Database connection closed.")
        
        if session_data:
            server_name, destination_server, destination_database, step10_tablename_files, step11_errors_folder, validation_folder_path11 = session_data
            print(f"Session data retrieved successfully: {server_name}, {destination_server}, {destination_database}, {step10_tablename_files}")
        else:
            print(f"No session data found for server {server_name} and database {database_name}.")
            return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    except sqlite3.Error as e:
        print(f"Database error: {str(e)}")
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        print(f"General error: {str(e)}")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    # Start time tracking
    start_time = time.time()

    # Prepare the paths for log and validation files
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(step11_errors_folder, f"{database_name}_step11_errors_{timestamp}.txt")
    validation_file_path = os.path.join(validation_folder_path11, f"{database_name}_Validation_step11_report_{timestamp}.txt")

    print(f"Log file path: {log_file_path}")
    print(f"Validation file path: {validation_file_path}")

    # Validate if source and destination connections are ready
    try:
        # Define source and destination database connection details
        src_server = destination_server
        src_database = destination_database
        dest_server = server_name
        dest_database = database_name

        # Connect to the source and destination SQL Servers
        src_conn = connect_to_server(src_server, src_database)
        dest_conn = connect_to_server(dest_server, dest_database)

        # List tables in the source database
        src_cursor = src_conn.cursor()
        list_tables(src_cursor)

        # Read table names from the file
        table_names_file = step10_tablename_files  # Path from session data
        try:
            with open(table_names_file, 'r') as f:
                table_names = [line.strip() for line in f.readlines() if line.strip()]
        except FileNotFoundError:
            print(f"Error: File not found: {table_names_file}")
            return jsonify({"error": f"Table names file not found: {table_names_file}"}), 404
        
        # Move each table's data from source to destination
        for table_name in table_names:
            print(f"Moving table: {table_name}")
            move_table(src_conn, dest_conn, table_name)

        # Validation: compare tables in the source and destination databases
        print("Performing validation between source and destination databases...")
        try:
            # Fetch table names from the source database
            src_cursor.execute("SELECT name FROM sys.tables")
            source_tables = set(row[0] for row in src_cursor.fetchall())

            # Fetch table names from the destination database
            dest_cursor = dest_conn.cursor()
            dest_cursor.execute("SELECT name FROM sys.tables")
            destination_tables = set(row[0] for row in dest_cursor.fetchall())

            # Compare the two sets of table names
            missing_tables = source_tables - destination_tables
            extra_tables = destination_tables - source_tables

            # Write the validation results to the validation file
            with open(validation_file_path, 'a') as validation_file:
                if not missing_tables and not extra_tables:
                    validation_file.write("Validation Successful: All table names match.\n")
                else:
                    if missing_tables:
                        validation_file.write(f"Missing Tables in Destination: {', '.join(missing_tables)}\n")
                    if extra_tables:
                        validation_file.write(f"Extra Tables in Destination: {', '.join(extra_tables)}\n")

            # Handle validation success or failure
            if not missing_tables and not extra_tables:
                validation_status = "Validation successful: All table names match."
            else:
                validation_status = "Validation failed: Some tables are missing or extra."
            print(f"Validation Status: {validation_status}")

        except Exception as e:
            print(f"Validation error: {str(e)}")
            with open(log_file_path, 'a') as log_file:
                log_file.write(f"\nValidation error: {str(e)}\n")
            return jsonify({"error": f"An error occurred during validation: {str(e)}"}), 500

    except pyodbc.Error as e:
        print(f"Error connecting to the database: {e}")
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"\nDatabase connection error: {str(e)}\n")
        return jsonify({"error": f"Database connection error: {str(e)}"}), 500
    except Exception as e:
        print(f"An error occurred: {e}")
        with open(log_file_path, 'a') as log_file:
            log_file.write(f"\nException occurred: {str(e)}\n")
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500
    finally:
        # Closing connections
        src_conn.close()
        dest_conn.close()

    # End time tracking
    end_time = time.time()
    time_taken = end_time - start_time  # Time in minutes
    minutes = int(time_taken // 60)
    seconds = int(time_taken % 60)
    milliseconds = int((time_taken * 1000) % 1000)
    print(f"Time taken for execution: {minutes} minutes {seconds} seconds")

    return jsonify({
        "status": validation_status,
        "validation_file": validation_file_path,
        "log_file": log_file_path,
        "time_taken": time_taken,
        "minutes": minutes,
        "seconds": seconds,
        "milliseconds": milliseconds
    })




@app.route('/run_powershell12', methods=['GET'])
def run_powershell12():
    # Get the database name from the request
    database_name = request.args.get('database')
    try:
        # Connecting to the SQLite database to retrieve session data
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        cursor.execute('''SELECT server_name, step12_errors_folder, validation_folder_path12,step10_tablename_files FROM sessions WHERE database_name=? AND folder_created=1''',
                  (database_name,))
        session_data = cursor.fetchone()
        conn.close()

        if session_data:
            server_name, step12_errors_folder, validation_folder_path12, step10_tablename_files = session_data
        else:
            return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    except sqlite3.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    # PowerShell script path
    powershell_script_path = r'.\scripts\step12.ps1'

    if server_name and database_name:
        # Generate timestamp for unique file names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(step12_errors_folder, f"{database_name}_step12_errors_{timestamp}.txt")
        validation_file_path = os.path.join(validation_folder_path12, f"{database_name}_Validation_step12_report_{timestamp}.txt")

        try:
            # Start time tracking
            start_time = time.time()

            # Run the PowerShell script
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                 "-ServerName", server_name, "-databaseName", database_name],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            # Capture the output and errors
            stdout, stderr = process.communicate()

            # Decode stdout and stderr to string
            stdout = stdout.decode("utf-8")
            stderr = stderr.decode("utf-8")

            # Log the PowerShell output for debugging purposes
            print("PowerShell script output:", stdout)
            print("PowerShell script errors:", stderr)

            # Calculate execution time
            end_time = time.time()
            time_taken = end_time - start_time  # Time in minutes
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Time taken: {minutes} minutes {seconds} seconds")

            # Log errors to the log file if any errors occurred
            if stderr:
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nPowerShell Errors:\n")
                    log_file.write(stderr)
                # Calculate execution time
                end_time = time.time()
                time_taken = end_time - start_time  # Time in minutes
                minutes = int(time_taken // 60)
                seconds = int(time_taken % 60)
                milliseconds = int((time_taken * 1000) % 1000)
                print(f"Time taken: {minutes} minutes {seconds} seconds")
                return jsonify({
                    "status": f"PowerShell script executed with errors.",
                    "log_file": log_file_path,
                    "stderr": stderr,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })

            # Validation logic for checking if foreign keys exist
            validation_status = ""
            try:
                # Perform database query to check foreign keys
                conn = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;')
                cursor = conn.cursor()

                # Check for the presence of foreign keys in the database
                cursor.execute("""
                    SELECT fk.name AS foreign_key_name,
                           tp.name AS parent_table,
                           ref.name AS referenced_table
                    FROM sys.foreign_keys fk
                    INNER JOIN sys.tables tp ON fk.parent_object_id = tp.object_id
                    INNER JOIN sys.tables ref ON fk.referenced_object_id = ref.object_id
                """)
                foreign_keys = cursor.fetchall()

                # Check if any foreign keys were found
                if foreign_keys:
                    validation_status = "Validation successful: Foreign keys are present."
                else:
                    validation_status = "Validation failed: No foreign keys found in the database."

                # Write validation results to the validation file
                with open(validation_file_path, 'a') as validation_file:
                    validation_file.write(f"Foreign Key Validation Status: {validation_status}\n")
                
                conn.close()

            except Exception as e:
                # If any error occurs during foreign key validation
                validation_status = f"Error during validation: {str(e)}"
                with open(log_file_path, 'a') as log_file:
                    log_file.write(f"\nError during foreign key validation: {str(e)}\n")
                end_time = time.time()
                time_taken = end_time - start_time
                minutes = int(time_taken // 60)
                seconds = int(time_taken % 60)
                milliseconds = int((time_taken * 1000) % 1000)
                print(f"Time taken for PowerShell execution: {minutes} minutes {seconds} seconds")

                return jsonify({
                    "status": f"PowerShell script executed with errors.",
                    "log_file": log_file_path,
                    "stderr": stderr,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })    
            end_time = time.time()
            time_taken = end_time - start_time
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            # Return response with the results
            return jsonify({
                "status": "completed",  # Explicit status indicating completion
                "stdout": stdout,
                "stderr": stderr,
                "execution_time": time_taken,
                "validation_status": validation_status,
                "log_file": log_file_path,
                "validation_file": validation_file_path,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": milliseconds
            })

        except Exception as e:
            return jsonify({"error": f"An error occurred while running the PowerShell script: {str(e)}"}), 500

    else:
        return jsonify({"error": "Missing parameters: server, database."}), 400


@app.route('/run_powershell13', methods=['GET'])
def run_powershell13():
    # Get the database name from the request
    print("start")
    database_name = request.args.get('database')
    try:
        # Connecting to the SQLite database to retrieve session data
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        cursor.execute('''SELECT server_name, step13_errors_folder, validation_folder_path13,corp_names_folder_path FROM sessions WHERE database_name=? AND folder_created=1''',
                  (database_name,))
        session_data = cursor.fetchone()
        conn.close()

        if session_data:
            server_name, step13_errors_folder, validation_folder_path13, corp_names_folder_path = session_data
        else:
            return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    except sqlite3.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    # PowerShell script path
    powershell_script_path = r'.\scripts\step13.ps1'

    if server_name and database_name:
        # Generate timestamp for unique file names
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(step13_errors_folder, f"{database_name}_step13_errors_{timestamp}.txt")
        validation_file_path = os.path.join(validation_folder_path13, f"{database_name}_Validation_step13_report_{timestamp}.txt")

        try:
            # Start time tracking
            start_time = time.time()

            # Run the PowerShell script
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                 "-serverName", server_name, "-databaseName", database_name,"-outputDirectory",corp_names_folder_path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            # Capture the output and errors
            stdout, stderr = process.communicate()

            # Decode stdout and stderr to string
            stdout = stdout.decode("utf-8")
            stderr = stderr.decode("utf-8")

            # Log the PowerShell output for debugging purposes
            print("PowerShell script output:", stdout)
            print("PowerShell script errors:", stderr)
            # Set validation_status based on the presence of errors
            if stderr:
                validation_status = "Failed"
            else:
                validation_status = "Success"

            # Calculate execution time
            end_time = time.time()
            time_taken = end_time - start_time  # Time in minutes
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Time taken: {minutes} minutes {seconds} seconds")

            # Log errors to the log file if any errors occurred
            if stderr:
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nPowerShell Errors:\n")
                    log_file.write(stderr)
                # Calculate execution time
                end_time = time.time()
                time_taken = end_time - start_time  # Time in minutes
                minutes = int(time_taken // 60)
                seconds = int(time_taken % 60)
                milliseconds = int((time_taken * 1000) % 1000)
                print(f"Time taken: {minutes} minutes {seconds} seconds")
                return jsonify({
                    "status": f"PowerShell script executed with errors.",
                    "log_file": log_file_path,
                    "stderr": stderr,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })

                
            end_time = time.time()
            time_taken = end_time - start_time
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            # Return response with the results
            return jsonify({
                "status": "completed",  # Explicit status indicating completion
                "stdout": stdout,
                "stderr": stderr,
                "execution_time": time_taken,
                "validation_status": validation_status,
                "log_file": log_file_path,
                "validation_file": validation_file_path,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": milliseconds
            })

        except Exception as e:
            return jsonify({"error": f"An error occurred while running the PowerShell script: {str(e)}"}), 500

    else:
        return jsonify({"error": "Missing parameters: server, database."}), 400

import glob

@app.route('/run_powershell14', methods=['GET'])
def run_powershell14():
    print("start")
    database_name = request.args.get('database')
    try:
        conn = get_sql_server_connection()
        cursor = conn.cursor()
        cursor.execute('''SELECT server_name, step14_errors_folder, validation_folder_path14, corp_names_folder_path, corp_objects_folder_path 
                     FROM sessions WHERE database_name=? AND folder_created=1''', (database_name,))
        session_data = cursor.fetchone()
        conn.close()

        if session_data:
            server_name, step14_errors_folder, validation_folder_path14, corp_names_folder_path, corp_objects_folder_path = session_data
        else:
            return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    except sqlite3.Error as e:
        return jsonify({"error": f"Database error: {str(e)}"}), 500
    except Exception as e:
        return jsonify({"error": f"An error occurred: {str(e)}"}), 500

    # PowerShell script paths
    powershell_script_path1 = r'.\scripts\step14a.ps1'
    powershell_script_path2 = r'.\scripts\step14b.ps1'
    powershell_script_path3 = r'.\scripts\step14c.ps1'
    views = "Views"
    sps = "StoredProcedures"
    print("abc")
    if server_name and database_name:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(step14_errors_folder, f"{database_name}_step14_errors_{timestamp}.txt")
        validation_file_path = os.path.join(validation_folder_path14, f"{database_name}_Validation_step14_report_{timestamp}.txt")

        # Find the first .txt file in corp_names_folder_path (used for functions, views, sps)
        try:
            function_file = next((f for f in glob.glob(os.path.join(corp_names_folder_path, '*.txt')) if 'function' in os.path.basename(f).lower()), None)
            views_file = next((f for f in glob.glob(os.path.join(corp_names_folder_path, '*.txt')) if 'view' in os.path.basename(f).lower()), None)
            sps_file = next((f for f in glob.glob(os.path.join(corp_names_folder_path, '*.txt')) if 'sp' in os.path.basename(f).lower() or 'proc' in os.path.basename(f).lower()), None)

            if not function_file:
                return jsonify({"error": "No function list .txt file found in corp_names folder."}), 400
            if not views_file:
                return jsonify({"error": "No views list .txt file found in corp_names folder."}), 400
            if not sps_file:
                return jsonify({"error": "No stored procedures list .txt file found in corp_names folder."}), 400

        except Exception as e:
            return jsonify({"error": f"Error reading file from corp_names folder: {str(e)}"}), 500

        try:
            start_time = time.time()

            # Run step14a.ps1 (functions)
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path1,
                 "-serverName", server_name, "-databaseName", database_name,
                 "-outputRootFolder", corp_objects_folder_path, "-functionsListFile", function_file],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate()
            stdout, stderr = stdout.decode("utf-8"), stderr.decode("utf-8")
            print("PowerShell script output:", stdout)
            print("PowerShell script errors:", stderr)
            if stderr:
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nPowerShell Errors (Step 1):\n")
                    log_file.write(stderr)

            if stderr:
                return jsonify({
                    "status": f"PowerShell script executed with errors (Step 1).",
                    "log_file": log_file_path,
                    "stderr": stderr
                })

            # Run step14b.ps1 (views)
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path2,
                 "-serverName", server_name, "-databaseName", database_name,
                 "-outputRootFolder", corp_objects_folder_path, "-viewsFolder", views, "-viewsListFilePath", views_file],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate()
            stdout, stderr = stdout.decode("utf-8"), stderr.decode("utf-8")
            print("PowerShell script output:", stdout)
            print("PowerShell script errors:", stderr)
            if stderr:
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nPowerShell Errors (Step 2):\n")
                    log_file.write(stderr)

            if stderr:
                return jsonify({
                    "status": f"PowerShell script executed with errors (Step 2).",
                    "log_file": log_file_path,
                    "stderr": stderr
                })

            # Run step14c.ps1 (stored procedures)
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path3,
                 "-serverName", server_name, "-databaseName", database_name,
                 "-outputRootFolder", corp_objects_folder_path, "-storedProceduresFolder", sps, "-spListFilePath", sps_file],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout, stderr = process.communicate()
            stdout, stderr = stdout.decode("utf-8"), stderr.decode("utf-8")
            print("PowerShell script output:", stdout)
            print("PowerShell script errors:", stderr)
            if stderr:
                with open(log_file_path, 'a') as log_file:
                    log_file.write("\nPowerShell Errors (Step 3):\n")
                    log_file.write(stderr)

            end_time = time.time()
            time_taken = end_time - start_time
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)

            if stderr:
                return jsonify({
                    "status": f"PowerShell script executed with errors (Step 3).",
                    "log_file": log_file_path,
                    "stderr": stderr,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })

            return jsonify({
                "status": "completed",
                "stdout": stdout,
                "stderr": stderr,
                "execution_time": time_taken,
                "log_file": log_file_path,
                "validation_file": validation_file_path,
                "minutes": minutes,
                "seconds": seconds,
                "milliseconds": milliseconds
            })

        except Exception as e:
            return jsonify({"error": f"An error occurred while running the PowerShell scripts: {str(e)}"}), 500

    else:
        return jsonify({"error": "Missing parameters: server, database."}), 400


@app.route('/run_powershell15', methods=['GET'])
def run_powershell15():
    #server_name = request.args.get('server')
    database_name = request.args.get('database')
    #db_errors_folder_path = session.get('db_errors_folder')
    #scripts_folder_path = session.get('scripts_folder_path')
    #validation_folder_path5 = session.get('validation_folder_path5')
    print("15------------------------------------------------------------------")
    # Check if folder has been created
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT server_name,step14_errors_folder,folder_path, corp_objects_folder_path, validation_folder_path14,schemanamefrom,schemanameto 
                 FROM sessions WHERE  database_name=? AND folder_created=1''',
              (database_name,))
    session_data = cursor.fetchone()
    conn.close()
    #scripts_folder_path = scripts_folder_path
    print("sql")
    if session_data:
        server_name,step14_errors_folder_path,folder_path,corp_objects_folder_path, validation_folder_path14,schemanamefrom,schemanameto = session_data
        print(server_name,step14_errors_folder_path,corp_objects_folder_path,folder_path, validation_folder_path14)
        print("-------------------------------------------------------------------------------------------")
        print(schemanamefrom,schemanameto)
    else:
        return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400

    powershell_script_path = r".\scripts\step6.ps1"
    
    if server_name and database_name and step14_errors_folder_path:
        # Create a unique timestamp for the log file
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Declare log file paths
        log_file_path = os.path.join(step14_errors_folder_path, f"{database_name}_step14_errors_{timestamp}.txt")
        validation_file_path = os.path.join(validation_folder_path14, f"{database_name}_Validation_14b_Testowner_report_{timestamp}.txt")
        
        try:
            start_time = time.time()
            print("powershell execution started")
            # Running the PowerShell script via subprocess
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                 "-rootFolderPath", corp_objects_folder_path,"-Fromsch",schemanamefrom,"-Tosch",schemanameto],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            print("half execution")

            # Wait for the process to complete and capture its output
            stdout, stderr = process.communicate()
            print ("quATAR execution")
            # Decode stdout and stderr
            stdout = stdout.decode("utf-8")
            stderr = stderr.decode("utf-8")
            print("powershell executed")
            # Check if any errors were produced during the execution of the PowerShell script
            if stderr:
                # Log the error in the file only if the process encountered an issue
                with open(log_file_path, 'a',errors='ignore') as log_file:
                    log_file.write("\nPowerShell Errors:\n")
                    log_file.write(stderr)
                    end_time = time.time()
                    time_taken = end_time - start_time  # Time in seconds

                    # Convert time_taken into minutes, seconds, and milliseconds
                    minutes = int(time_taken // 60)
                    seconds = int(time_taken % 60)
                    milliseconds = int((time_taken * 1000) % 1000)
                    print(f"Time taken: {time_taken:.2f} minutes")

                return jsonify({
                    "status": "PowerShell script executed with errors.",
                    "log_file": log_file_path,
                    "time_taken": time_taken,
                    "minutes": minutes,
                    "seconds": seconds,
                    "milliseconds": milliseconds
                })

            # If no errors from PowerShell, check if the files have been modified as expected
            # Check if the folder paths exist and contain SQL files
            sql_files = []
            for folder_path in os.listdir(corp_objects_folder_path):
                folder_full_path = os.path.join(corp_objects_folder_path, folder_path)
                if os.path.isdir(folder_full_path):
                    # Loop through the subfolder to find .sql files
                    for sql_file in os.listdir(folder_full_path):
                        # Skip files that start with "dbo"
                        if sql_file.endswith('.sql') and not sql_file.lower().startswith('dbo'):
                            sql_files.append(os.path.join(folder_full_path, sql_file))

            # If no SQL files are found, raise an error
            if not sql_files:
                raise ValueError(f"No SQL files found in the specified directory: {corp_objects_folder_path}")            

            folder = corp_objects_folder_path  # Using scripts folder path
            schema = "[testowner]"  # Replace with your desired schema name

            #add_schema_to_sql_objects(folder, schema)
            #add_schema_to_sql_objects1(folder, schema)
            # Validation errors for SQL files

            # Validation errors for SQL files
            validation_errors = []  # To store any validation errors for SQL files
            for sql_file in sql_files:
                file_path = os.path.join(corp_objects_folder_path, sql_file)
                try:
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                        lines = [file.readline().strip() for _ in range(8)]
                except UnicodeDecodeError:
                    # If 'utf-8' fails, try 'windows-1252' encoding
                    with open(file_path, 'r', encoding='windows-1252') as file:
                        lines = [file.readline().strip() for _ in range(8)]

                #with open(file_path, 'r') as file:
                    #lines = [file.readline().strip() for _ in range(8)]  # Read only the first 8 lines

                    # Check if the content was modified correctly
                    file_error = False  # Flag to track if there's an error for this file

                    # Check if schema was added correctly to the object names
                    for line in lines:
                        if re.search(r'(?i)(CREATE|ALTER)\s+(PROCEDURE|VIEW|FUNCTION|TRIGGER)\s+[^\[]*\w+\.[^\]]+', line):
                            # Check if the schema is added (i.e., it should look like schema_name.object_name)
                            if schema.lower() not in line.lower():
                                validation_errors.append(f"Error: Schema '{schema}' not added in {sql_file}")
                                file_error = True
                                break  # Stop checking further if an error is found

                    # Check that 'corpuser' was replaced with 'testowner'
                    for line in lines:
                        if 'corpuser' in line:
                            validation_errors.append(f"Error: 'corpuser' not replaced in {sql_file}")
                            file_error = True
                            break  # Stop checking further if an error is found

                    # Check if 'create' statements were replaced with 'alter'
                    for line in lines:
                        if any(keyword in line for keyword in ['create proc', 'create procedure', 'create view', 'create function', 'create trigger']):
                            validation_errors.append(f"Error: 'create' statements not replaced with 'alter' in {sql_file}")
                            file_error = True
                            break  # Stop checking further if an error is found

                    # If there's an error with this file, mark it
                    if file_error:
                        validation_errors.append(f"File: {sql_file} - ERROR\n")
                    else:
                        validation_errors.append(f"File: {sql_file} - Successfully modified\n")

            # Write the validation report
            with open(validation_file_path, 'a') as validation_file:
                if validation_errors:
                    validation_file.write("Validation failed for the following files:\n")
                    for error in validation_errors:
                        validation_file.write(f"{error}\n")
                else:
                    validation_file.write("Validation successful. All SQL files were modified correctly.\n")
            
            end_time = time.time()
            time_taken = end_time - start_time  # Time in seconds

                    # Convert time_taken into minutes, seconds, and milliseconds
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)

            print(f"Time taken: {time_taken:.2f} minutes")
            print("completed----------------------------------")
            return jsonify({
                            "status": "completed",
                            "stdout": stdout,
                            "stderr": stderr,
                            "execution_time": time_taken,
                            "log_file": log_file_path,
                            "validation_file": validation_file_path,
                            "minutes": minutes,
                            "seconds": seconds,
                            "milliseconds": milliseconds
            })            

        except Exception as e:
            # Handle any unexpected errors that occurred during the execution
            with open(log_file_path, 'a') as log_file:
                log_file.write("\nException:\n")
                log_file.write(str(e))
            return jsonify({"error": f"An error occurred: {str(e)}"})
        finally:
    # Make sure to calculate time even if there’s an error or early return
            end_time = time.time()
            time_taken = end_time - start_time  # Time in seconds

                    # Convert time_taken into minutes, seconds, and milliseconds
            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Time taken: {time_taken:.2f} minutes")

    return jsonify({"error": "Missing parameters: server, database, or folder path"}), 400



@app.route('/run_powershell16', methods=['GET'])
def run_powershell16():
    # Retrieve parameters from the query string or session
    database_name = request.args.get('database')
    print("16------------------------------------------------------------")
    conn = get_sql_server_connection()
    cursor = conn.cursor()
    cursor.execute('''SELECT server_name,step14_errors_folder,corp_objects_folder_path,folder_path, validation_folder_path14 
                 FROM sessions WHERE  database_name=? AND folder_created=1''',
              (database_name,))
    session_data = cursor.fetchone()
    conn.close()

    if session_data:
        server_name, step14_errors_folder_path, corp_objects_folder_path, folder_path, validation_folder_path14 = session_data
        print(server_name, step14_errors_folder_path, corp_objects_folder_path, folder_path, validation_folder_path14)
    else:
        return jsonify({"error": "Please create the folder first by submitting the folder path."}), 400
    print("Processing database:", database_name)
    
    cross_db_results = check_and_move_cross_db_files(corp_objects_folder_path, folder_path, database_name)
    
    # Path to the PowerShell script
    powershell_script_path = r".\scripts\step7.ps1"
    
    # Create a unique timestamp for the log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file_path = os.path.join(step14_errors_folder_path, f"{database_name}_ErrorLog_db_errors_{timestamp}.txt")
    validation_file_path = os.path.join(validation_folder_path14, f"{database_name}_Validation_14c_Testowner_report_{timestamp}.txt")

    total_time_taken = 0  # Track total time taken for all attempts

    try:
        # Run PowerShell script 3 times regardless of success or failure
        for attempt in range(3):  # Try exactly 3 times
            print(f"Attempt {attempt + 1} of 3 to run PowerShell script.")

            start_time = time.time()
            # Running the PowerShell script via subprocess
            process = subprocess.Popen(
                ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
                 "-serverName", server_name, "-databaseName", database_name, "-scriptParentFolder", corp_objects_folder_path, "-logFolder", step14_errors_folder_path],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )

            # Wait for the process to complete and capture its output
            stdout, stderr = process.communicate()

            # Decode stdout and stderr
            stdout = stdout.decode("utf-8")
            stderr = stderr.decode("utf-8")

            # Log output for the current attempt
            with open(log_file_path, 'a') as log_file:
                log_file.write(f"\nAttempt {attempt + 1} Output:\n")
                log_file.write(f"stdout:\n{stdout}\nstderr:\n{stderr}\n")

            # Time calculation for the current attempt
            end_time = time.time()
            time_taken = end_time - start_time  # Time in seconds
            total_time_taken += time_taken  # Add to total time taken

            minutes = int(time_taken // 60)
            seconds = int(time_taken % 60)
            milliseconds = int((time_taken * 1000) % 1000)
            print(f"Time taken for attempt {attempt + 1}: {minutes} minutes {seconds} seconds {milliseconds} ms")

        # After 3 attempts, proceed to validation
        total_files_created = None
        match = re.search(r"Total files created\s*[:\-]?\s*(\d+)", stdout)
        if match:
            total_files_created = int(match.group(1))  # Extract the number from the match
        else:
            total_files_created = 0  # Default value if no match is found

        print(f"Total Files Created: {total_files_created}")
        
        # Call the function add_schema_to_sql_objects1
        print("Calling add_schema_to_sql_objects1 function...")
        folder = corp_objects_folder_path  # Using scripts folder path
        schema = "[testowner]"  # Replace with your desired schema name
        add_schema_to_sql_objects1(folder, schema)
        print("Function add_schema_to_sql_objects1 executed successfully!")

        # **Debugging Log**: Ensure function execution completes before proceeding
        print("Now running PowerShell script again...")

        # Now run PowerShell script again
        print("Running PowerShell script again after function call...")
        process = subprocess.Popen(
            ["powershell", "-ExecutionPolicy", "Bypass", "-File", powershell_script_path,
             "-serverName", server_name, "-databaseName", database_name, "-scriptParentFolder", corp_objects_folder_path, "-logFolder", step14_errors_folder_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        # Wait for the process to complete and capture its output
        stdout, stderr = process.communicate()

        # Decode stdout and stderr
        stdout = stdout.decode("utf-8")
        stderr = stderr.decode("utf-8")

        # Log the output
        with open(log_file_path, 'a') as log_file:
            log_file.write("\nSecond PowerShell Execution Output:\n")
            log_file.write(f"stdout:\n{stdout}\nstderr:\n{stderr}\n")

        # Count files in all subfolders
        def count_files_in_subfolders(base_folder):
            total_files = 0
            for dirpath, _, filenames in os.walk(base_folder):
                total_files += len(filenames)
            return total_files

        total_files_in_subfolders = count_files_in_subfolders(corp_objects_folder_path)

        # Connect to the database and get the count of objects
        conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server_name};DATABASE={database_name};Trusted_Connection=yes;'
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()

        cursor.execute("select count(*) from sys.objects where type in ('P', 'V', 'FN','FS','FT','TR')")
        result = cursor.fetchone()
        object_count = result[0] if result else 0

        # Open the validation file for writing
        with open(validation_file_path, 'a') as validation_file:
            validation_file.write(f"SQL Query Result: {object_count} \n")
            validation_file.write(f"Total Files Created (from PowerShell): {total_files_in_subfolders} \n")
            print(object_count, total_files_in_subfolders)

            # Compare the counts
            if object_count == total_files_in_subfolders:
                validation_file.write("Validation Successful: Counts match.\n")
            else:
                validation_file.write("Validation Failed: Mismatch in counts.\n")

        # Time calculation after all attempts
        end_time = time.time()
        total_minutes = int((total_time_taken // 60) % 60)
        total_seconds = int(total_time_taken % 60)
        total_milliseconds = int((total_time_taken * 1000) % 1000)
        print(f"Total Time Taken for 3 attempts: {total_minutes} minutes {total_seconds} seconds {total_milliseconds} ms")
        
        return jsonify({
            "status": "completed",
            "validation_file": validation_file_path,
            "total_time_taken": total_time_taken,
            "minutes": total_minutes,
            "seconds": total_seconds,
            "milliseconds": total_milliseconds
        })

    except Exception as e:
        # Handle any unexpected errors and log them
        with open(log_file_path, 'a') as log_file:
            log_file.write("\nException:\n")
            log_file.write(str(e))
        return jsonify({"error": f"An error occurred: {str(e)}"})

    finally:
        # Ensure the total time is calculated regardless of early exits or errors
        total_minutes = int((total_time_taken // 60) % 60)
        total_seconds = int(total_time_taken % 60)
        total_milliseconds = int((total_time_taken * 1000) % 1000)
        print(f"Total Time Taken for all attempts: {total_minutes} minutes {total_seconds} seconds {total_milliseconds} ms")



@app.route('/database_page', methods=['GET'])
def database_page():
    server_name = request.args.get('server')
    database_name = request.args.get('database')
     
     
     
     # Path to the script
    powershell_script_path = r".\scripts\step1.ps1"
    powershell_script_path2 = r".\scripts\step2.ps1"
    powershell_script_path3 = r".\scripts\step3.ps1"
    powershell_script_path4 = r".\scripts\step4.ps1"
    powershell_script_path5 = r".\scripts\step5.ps1"
    powershell_script_path6 = r".\scripts\step6.ps1"
    powershell_script_path7 = r".\scripts\step7.ps1"
    powershell_script_path8 = r".\scripts\step8.ps1"
    # Extract script name from the path
    script_name = os.path.basename(powershell_script_path)
    script_name2 = os.path.basename(powershell_script_path2)
    script_name3 = os.path.basename(powershell_script_path3)
    script_name4 = os.path.basename(powershell_script_path4)
    script_name5 = os.path.basename(powershell_script_path5)
    script_name6 = os.path.basename(powershell_script_path6)
    script_name7 = os.path.basename(powershell_script_path7)
    script_name8 = os.path.basename(powershell_script_path8)
    
    return render_template('db_refresh.html', server=server_name, database=database_name,script_name2=script_name2,script_name=script_name,script_name3=script_name3, script_name4=script_name4,script_name5=script_name5,script_name6=script_name6,script_name7=script_name7,script_name8=script_name8)


REPL_CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=LAPTOP-3AU3RIT3;"
    "DATABASE=Dbrefresh;"
    "Trusted_Connection=yes;"
)


 
def get_replication_connection():
    return pyodbc.connect(REPL_CONN_STR)
 
@app.route('/replication_dashboard')
def dashboard():
    # Optional: protect with login like your other pages
    if 'login_name' not in session:
        return redirect(url_for('login'))
 
    sql_query = "SELECT * FROM Get_Replication_details"
 
    # Get replication data
    with get_replication_connection() as conn:
        df = pd.read_sql(sql_query, conn)
 
    # Get column indexes for filter JS
    pub_col_index = df.columns.get_loc("publication_name")
    art_col_index = df.columns.get_loc("article")
 
    # Render HTML table
    detail_html = df.to_html(
        classes='table table-hover',
        index=False,
        table_id="sql_table",
        justify='left'
    )
 
    return render_template(
        'replication_details.html',
        table=detail_html,
        pub_col_index=pub_col_index,
        art_col_index=art_col_index
    )
 
 
@app.route('/replication_dashboard/summary')
def dashboard_summary():
    if 'login_name' not in session:
        return redirect(url_for('login'))
 
    sql_query = "SELECT * FROM Get_Replication_details"
 
    with get_replication_connection() as conn:
        df = pd.read_sql(sql_query, conn)
 
    # Grouped summary
    summary_df = (
        df.groupby(
            [
                "publication_server",
                "publisher_db",
                "publication_name",
                "subscription_server",
                "subscriber_db",
            ]
        )
        .agg(article_count=("article", "count"))
        .reset_index()
    )
 
    summary_html = summary_df.to_html(
        classes='table table-hover',
        index=False,
        justify='left'
    )
 
    return render_template(
        'replication_summary.html',
        summary_table=summary_html
    )

def get_db_connection_POC():
    conn = pyodbc.connect(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=LAPTOP-3AU3RIT3;"
        "DATABASE=DBrefresh;"
        "Trusted_Connection=yes;")
    return conn
 

@app.route('/get-pocs', methods=['GET'])
def get_pocs():
    application = request.args.get('application','').strip()

    if not application:
        return jsonify({"pocs":[]})
    sql = """
    select DISTINCT POC from (
    select distinct
    txtServerName,txtDBName,txtAppName as [Application] ,txtPM as POC
    from dbinitobjectdetail a inner join
    dbinitserverdbmapping b on a.intServerDBMappingID=b.intServerDBMappingID
    inner join dbinitownershipdetail c on a.txtObjName=c.txtObjName) AS sub
    where sub.Application LIKE ?
    ORDER BY sub.POC;
    """
    conn = get_db_connection_POC()
    cursor = conn.cursor()
    pattern =f"{application}"
    cursor.execute(sql,(pattern,))
    rows=cursor.fetchall()
    pocs = [row[0] for row in rows if row[0] is not None]
    cursor.close()
    conn.close()

    return jsonify({"pocs": pocs})

@app.route('/get-applications', methods=['GET'])
def get_applications():
    sql = """
    select DISTINCT Application_Name  from (
    select distinct
    txtServerName,txtDBName,txtAppName as Application_Name ,txtPM as POC
    from dbinitobjectdetail a inner join
    dbinitserverdbmapping b on a.intServerDBMappingID=b.intServerDBMappingID
    inner join dbinitownershipdetail c on a.txtObjName=c.txtObjName) sub
    order by sub.Application_Name
    """
    conn = get_db_connection_POC()
    cursor = conn.cursor()
    cursor.execute(sql)
    apps = [row[0] for row in cursor.fetchall() if row[0] is not None]
    cursor.close()
    conn.close()
 
    return jsonify({"applications": apps})
     
 


# Run the Flask application
if __name__ == '__main__':
    init_db()
    app.run(host='0.0.0.0', port=8099,debug='True')

#app.run(host='0.0.0.0', port=5000)
