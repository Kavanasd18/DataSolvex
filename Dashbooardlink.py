import pyodbc
import pandas as pd
from flask import Flask, render_template_string
 
# 1️⃣ SQL Server connection
conn_str = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=ISCLSCDBT9\DIST;"
    "DATABASE=distribution;"
    "Trusted_Connection=yes;"
)
conn = pyodbc.connect(conn_str)
 
#2️⃣ Flask app
app = Flask(__name__)
 
# 3️⃣ Shared styling template
BASE_STYLE = """
<style>
    body {
        margin: 0;
        background: linear-gradient(to right, #f0f4f7, #e0ebf8);
        font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
    }
    .navbar {
        background-color: #1a73e8;
        color: white;
        padding: 15px 40px;
        display: flex;
        justify-content: space-between;
        align-items: center;
        box-shadow: 0 4px 10px rgba(0,0,0,0.1);
        position: sticky;
        top: 0;
        z-index: 1000;
    }
    .navbar a {
        color: white;
        text-decoration: none;
        font-weight: 500;
        margin-right: 25px;
        transition: 0.3s;
    }
    .navbar a:hover {
        color: #cce5ff;
    }
    .navbar h2 {
        margin: 0;
        font-size: 1.5rem;
        font-weight: 600;
    }
    .content {
        margin: 40px;
    }
    h3 {
        color: #1a73e8;
        font-weight: bold;
        text-shadow: 1px 1px #b0c4de;
    }
    .table-container {
        background-color: white;
        padding: 25px;
        border-radius: 15px;
        box-shadow: 0 6px 20px rgba(0,0,0,0.1);
        overflow-x: auto;
        margin-bottom: 30px;
    }
    table {
        width: 100%;
        border-collapse: collapse;
        min-width: 800px;
    }
    table th {
        background-color: #cce5ff;
        color: #0d3a66;
        font-weight: 600;
        text-align: left;
        padding: 12px;
        border: 1px solid #b8d7f0;
    }
    table td {
        text-align: left;
        vertical-align: middle;
        padding: 12px;
        border: 1px solid #d0e7ff;
    }
    table tbody tr:hover td {
        background-color: #e6f2ff;
    }
    .filter-input {
        margin-bottom: 15px;
        width: 48%;
        padding: 10px 15px;
        border: 1px solid #b8d7f0;
        border-radius: 10px;
        box-shadow: inset 0 1px 3px rgba(0,0,0,0.05);
        transition: all 0.3s ease;
        font-size: 0.95rem;
    }
    .filter-input:focus {
        border-color: #1a73e8;
        box-shadow: 0 0 8px rgba(26,115,232,0.2);
        outline: none;
        background-color: #f0f8ff;
    }
</style>
"""
 
# 4️⃣ Details page
DETAIL_TEMPLATE = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>SQL Replication Dashboard</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    {BASE_STYLE}
<script>
        function filterTable() {{
            let pubFilter = document.getElementById("publication_name_filter").value.toLowerCase();
            let artFilter = document.getElementById("article_filter").value.toLowerCase();
            let table = document.getElementById("sql_table");
            let tr = table.getElementsByTagName("tr");
 
            for (let i = 1; i < tr.length; i++) {{
                let tdPub = tr[i].getElementsByTagName("td")[{{{{ pub_col_index }}}}];
                let tdArt = tr[i].getElementsByTagName("td")[{{{{ art_col_index }}}}];
                let showRow = true;
 
                if (tdPub && pubFilter) {{
                    if (tdPub.innerText.toLowerCase().indexOf(pubFilter) === -1) {{
                        showRow = false;
                    }}
                }}
 
                if (tdArt && artFilter) {{
                    if (tdArt.innerText.toLowerCase().indexOf(artFilter) === -1) {{
                        showRow = false;
                    }}
                }}
 
                tr[i].style.display = showRow ? "" : "none";
            }}
        }}
</script>
</head>
<body>
<div class="navbar">
<h2>SQL Replication Dashboard</h2>
<div>
<a href="/">Details</a>
<a href="/summary">Summary</a>
<a href="/" onclick="window.location.reload(); return false;" class="btn btn-light btn-sm text-primary fw-bold">Refresh</a>
</div>
</div>
 
    <div class="content">
<div class="d-flex justify-content-between mb-3 flex-wrap">
<input type="text" id="publication_name_filter" class="filter-input" placeholder="Filter by Publication Name" onkeyup="filterTable()">
<input type="text" id="article_filter" class="filter-input" placeholder="Filter by Article" onkeyup="filterTable()">
</div>
 
        <div class="table-container">
<h3>Replication Details</h3>
            {{{{ table | safe }}}}
</div>
</div>
</body>
</html>
"""
 
# 5️⃣ Summary page
SUMMARY_TEMPLATE = f"""
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Replication Summary</title>
<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    {BASE_STYLE}
</head>
<body>
<div class="navbar">
<h2>SQL Replication Dashboard</h2>
<div>
<a href="/">Details</a>
<a href="/summary">Summary</a>
</div>
</div>
 
    <div class="content">
<div class="table-container">
<h3>Replication Summary Overview</h3>
            {{{{ summary_table | safe }}}}
</div>
</div>
</body>
</html>
"""
 


# 6️⃣ Flask routes
@dasdh.route('/')
def home():
    sql_query = "SELECT * FROM Get_Replication_details"
    df = pd.read_sql(sql_query, conn)
 
    pub_col_index = df.columns.get_loc("publication_name")
    art_col_index = df.columns.get_loc("article")
 
    detail_html = df.to_html(classes='table table-hover', index=False, table_id="sql_table", justify='left')
 
    return render_template_string(
        DETAIL_TEMPLATE,
        table=detail_html,
        pub_col_index=pub_col_index,
        art_col_index=art_col_index
    )
 
 
@app.route('/summary')
def summary():
    sql_query = "SELECT * FROM Get_Replication_details"
    df = pd.read_sql(sql_query, conn)
 
    summary_df = (
        df.groupby(["publication_server", "publisher_db", "publication_name", "subscription_server", "subscriber_db"])
        .agg(article_count=("article", "count"))
        .reset_index()
    )
 
    summary_html = summary_df.to_html(classes='table table-hover', index=False, justify='left')
    return render_template_string(SUMMARY_TEMPLATE, summary_table=summary_html)
 
 
