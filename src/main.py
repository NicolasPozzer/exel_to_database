import pyodbc as db
import pandas as pd
from src.components.data_validate import create_table_from_df, insert_data_from_df, save_invalid_rows, \
    validate_dataframe, validate_data_types

# Connection to Database
try:
    conn_sql = db.connect(
        driver="SQL Server",
        server="DESKTOP-NICO",
        database="exel_to_db",  # Name of the database in SQL Server
        user="",
        password=""
    )
    print("Successful connection to SQL Server!")
except db.Error as ex:
    print("Error connecting to SQL Server!", ex)

# Program logic
try:
    # Read the Excel file
    file_path = 'Libro1.xlsx'
    df = pd.read_excel(file_path)

    # Validate the DataFrame and get valid and invalid rows
    valid_rows, invalid_rows, first_row_types = validate_dataframe(df)

    # If there are invalid rows, save them to an Excel file with error formatting
    if not invalid_rows.empty:
        save_invalid_rows(invalid_rows, 'invalid_rows.xlsx', df, first_row_types)
        print("Invalid rows found. File 'invalid_rows.xlsx' created.")

    else:
        try:
            cursor = conn_sql.cursor()

            # Name of the table in the SQL Server database
            table_name = 'exel_data'

            # Create the table in SQL Server based on the DataFrame of valid rows
            create_table_from_df(cursor, table_name, valid_rows)
            print(f"Table '{table_name}' successfully created in SQL Server!")

            # Insert valid data into the SQL Server table
            insert_data_from_df(cursor, table_name, valid_rows)
            print(f"Data successfully inserted into the table '{table_name}' in SQL Server!")

            # Commit transaction
            cursor.commit()

        except Exception as sql_ex:
            print(f"Error executing SQL operations: {sql_ex}")

        finally:
            # Close cursor and connection to SQL Server
            if 'cursor' in locals():
                cursor.close()
            conn_sql.close()

except Exception as ex:
    print("Error executing the program:", ex)
