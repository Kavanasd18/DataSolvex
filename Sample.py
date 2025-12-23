import pyodbc

# Define the connection details
server = 'ISMGMTDBP2'  # e.g., 'localhost' or '192.168.1.1'
database = 'EPMDBCCM'  # e.g., 'TestDB'
username = 'EPMDBCCM'  # e.g., 'sa'
password = 'SwpV4554+aqftk'  # e.g., 'password123'

try:
    conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};'
                          f'SERVER={server};'
                          f'DATABASE={database};'
                          f'UID={username};'
                          f'PWD={password}')
    
    cursor = conn.cursor()
    cursor.execute('SELECT top 1 * FROM sys.tables')
    rows = cursor.fetchall()
    
    for row in rows:
        print(row)
    
except pyodbc.Error as e:
    print(f"An error occurred: {e}")
finally:
    # Ensure the connection and cursor are closed even if an error occurs
    cursor.close()
    conn.close()

