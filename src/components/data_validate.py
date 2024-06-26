import pandas as pd

# Validates that all rows have the same data types as the first row
def validate_data_types(df):
    first_row_types = df.iloc[0].apply(lambda x: pd.api.types.infer_dtype([x]))

    for col in df.columns:
        col_type = first_row_types[col]
        if not df[col].apply(lambda x: pd.api.types.infer_dtype([x])).eq(col_type).all():
            return False
    return True

def get_sql_type(dtype):
    """Maps pandas data types to SQL Server data types"""
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BIT"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "NVARCHAR(MAX)"

def detect_data_types(df):
    """Detects the data types of the columns in the DataFrame"""
    columns = df.columns
    types = {}

    for col in columns:
        # Iterate over the first rows until finding a non-null value to determine the type
        for value in df[col]:
            if not pd.isna(value):
                types[col] = pd.api.types.infer_dtype([value])
                break

    return types

def create_table_from_df(cursor, table_name, df):
    # Creates a table in SQL Server based on the pandas DataFrame
    types = detect_data_types(df)
    columns_with_types = [f"{col} {get_sql_type(types[col])}" for col in df.columns]
    create_table_query = f"""
    CREATE TABLE {table_name} (
        {', '.join(columns_with_types)}
    )
    """
    cursor.execute(create_table_query)

def insert_data_from_df(cursor, table_name, df):
    # Inserts data from a DataFrame into a SQL Server table
    insert_query = f"INSERT INTO {table_name} ({', '.join(df.columns)}) VALUES ({', '.join(['?' for _ in df.columns])})"

    for index, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))
    cursor.commit()

# Function to validate a row
def validate_row(row, first_row_types):
    """Validates a row and returns a list of errors and columns with errors"""
    errors = []
    error_columns = []

    # Iterate over the columns of the row
    for col, value in row.items():
        # Check if the value is null
        if pd.isna(value):
            errors.append(f"{col}: NULL VALUE")
            error_columns.append(col)
            print(f"NULL VALUE IN: '{col}'")

        # Check if the data type is incorrect
        elif pd.api.types.infer_dtype([value]) != first_row_types[col]:
            errors.append(f"{col}: Incorrect data type")
            error_columns.append(col)
            print(f"INCORRECT DATA TYPE IN: '{col}'")

    return errors, error_columns


def validate_dataframe(df):
    """Validates the DataFrame and separates valid and invalid rows"""
    first_row_types = df.iloc[0].apply(lambda x: pd.api.types.infer_dtype([x]))
    errors_and_columns = df.apply(lambda row: validate_row(row, first_row_types), axis=1)
    errors_list = errors_and_columns.apply(lambda x: x[0])
    error_columns_list = errors_and_columns.apply(lambda x: x[1])
    valid_rows = df[errors_list.str.len() == 0]
    invalid_rows = df[errors_list.str.len() > 0]
    error_columns = error_columns_list[errors_list.str.len() > 0]
    return valid_rows, invalid_rows, first_row_types, error_columns


def save_invalid_rows(invalid_rows, file_path, original_df, first_row_types, error_columns):
    """Saves the invalid rows in an Excel file, marked with a color and with error details"""
    invalid_rows = invalid_rows.copy()
    invalid_rows['Error Columns'] = error_columns.apply(lambda cols: ', '.join(cols) if isinstance(cols, list) else '')

    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        original_df.to_excel(writer, sheet_name='Errors', index=False)
        workbook = writer.book
        worksheet = writer.sheets['Errors']
        format1 = workbook.add_format({'bg_color': '#FFCCCC'})

        # Mark the rows with errors in red
        for index in invalid_rows.index:
            worksheet.set_row(index + 1, None, format1)

        # Write the "Error Columns" header
        error_col_idx = len(original_df.columns)
        worksheet.write(0, error_col_idx, 'Error Columns')

        # Write the errors in the corresponding rows
        for row_idx in invalid_rows.index:
            worksheet.write(row_idx + 1, error_col_idx, invalid_rows.at[row_idx, 'Error Columns'])