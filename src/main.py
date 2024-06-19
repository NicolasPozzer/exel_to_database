import pyodbc as db
import pandas as pd
from components import queries

# Connection to Database
try:
    conn_sql = db.connect(
        driver="SQL Server",
        server="DESKTOP-NICO",
        database="exel_to_db"  # Name DB SQL Server
    )
    print("Successful connection to SQL Server!")
except db.Error as ex:
    print("Error Connecting to SQL Server!", ex)


try:
    cursor = conn_sql.cursor()

    cursor.execute("SELECT * FROM test")

    print(cursor.fetchall())





except Exception as ex:
    print("Error executing:", ex)
finally:
    # Close connections
    if 'conn_sql' in locals():
        conn_sql.close()