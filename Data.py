import pyodbc
 
# === CONFIGURATION ===
TABLES_FILE = r"E:\DB Refresh-2025\TestDB\nam.txt"
 
# Example connection strings (use Windows Authentication or add UID/PWD)
SOURCE_CONN_STR = "Driver={ODBC Driver 17 for SQL Server};Server=isclsdbt01\inst1;Database=testdb;Trusted_Connection=yes;"
DEST_CONN_STR = "Driver={ODBC Driver 17 for SQL Server};Server=ismgmtdbp2\inst5;Database=corpdata;Trusted_Connection=yes;"
 
# === FUNCTIONS ===
 
def get_tables_from_file(filename):
    with open(filename, 'r') as f:
        lines = [line.strip() for line in f if line.strip()]
    return [tuple(line.split('.')) for line in lines if '.' in line]
 
def has_identity_column(cursor, schema, table):
    query = f"""
        SELECT 1
        FROM sys.columns c
        JOIN sys.tables t ON c.object_id = t.object_id
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = ? AND t.name = ? AND c.is_identity = 1
    """
    cursor.execute(query, (schema, table))
    return cursor.fetchone() is not None
 
def fetch_data(src_cursor, schema, table):
    query = f"SELECT * FROM [{schema}].[{table}]"
    src_cursor.execute(query)
    columns = [column[0] for column in src_cursor.description]
    rows = src_cursor.fetchall()
    return columns, rows
 
def insert_data(dest_cursor, schema, table, columns, rows, identity_insert=False):
    col_names = ", ".join(f"[{col}]" for col in columns)
    placeholders = ", ".join("?" for _ in columns)
 
    if identity_insert:
        dest_cursor.execute(f"SET IDENTITY_INSERT [{schema}].[{table}] ON")
 
    for row in rows:
        insert_query = f"INSERT INTO [{schema}].[{table}] ({col_names}) VALUES ({placeholders})"
        dest_cursor.execute(insert_query, row)
 
    if identity_insert:
        dest_cursor.execute(f"SET IDENTITY_INSERT [{schema}].[{table}] OFF")
 
def main():
    tables = get_tables_from_file(TABLES_FILE)
 
    src_conn = pyodbc.connect(SOURCE_CONN_STR)
    dest_conn = pyodbc.connect(DEST_CONN_STR)
 
    src_cursor = src_conn.cursor()
    dest_cursor = dest_conn.cursor()
 
    for schema, table in tables:
        print(f"Processing: {schema}.{table}")
 
        try:
            identity_col = has_identity_column(dest_cursor, schema, table)
            print(f" - Identity column found: {identity_col}")
 
            columns, rows = fetch_data(src_cursor, schema, table)
            print(f" - Rows fetched: {len(rows)}")
 
            insert_data(dest_cursor, schema, table, columns, rows, identity_insert=identity_col)
            dest_conn.commit()
            print(f" - Data inserted successfully.")
        except Exception as e:
            print(f" - Error processing {schema}.{table}: {e}")
            dest_conn.rollback()
 
    src_conn.close()
    dest_conn.close()
 
if __name__ == "__main__":
    main()