from src.components.data_validate import validate_dataframe, \
save_invalid_rows, create_table_from_df, insert_data_from_df
import uvicorn
from fastapi import FastAPI, File, UploadFile, Form, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
import pandas as pd
import pyodbc as db
import os
import uuid  # To generate unique names for error files

app = FastAPI()
templates = Jinja2Templates(directory="templates")
app.mount("/static", StaticFiles(directory="static"), name="static")


# Route for the main page
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


# Route to handle file upload
@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...), table_name: str = Form(...)):
    try:
        if file.filename == '':
            return templates.TemplateResponse("index.html", {"request": request, "error": "No file selected"})

        # Ensure the uploads directory exists
        upload_dir = "static/uploads"
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir)

        # Save the uploaded file
        file_path = os.path.join(upload_dir, file.filename)
        with open(file_path, "wb") as buffer:
            buffer.write(file.file.read())

        # Process the file
        df = pd.read_excel(file_path)

        valid_rows, invalid_rows, first_row_types, error_columns = validate_dataframe(df)

        if not invalid_rows.empty:
            # Generate unique name for error file
            error_file_name = f"invalid_rows_{uuid.uuid4()}.xlsx"
            error_file_path = os.path.join(upload_dir, error_file_name)
            save_invalid_rows(invalid_rows, error_file_path, df, first_row_types, error_columns)
            error_file_url = f"/static/uploads/{error_file_name}"
            return templates.TemplateResponse("index.html", {"request": request, "error": "Invalid rows found.",
                                                             "error_file_url": error_file_url})

        # DB Params
        conn_sql = db.connect(
            driver="SQL Server",
            server="DESKTOP-NICO",
            database="exel_to_db",
            user="",
            password=""
        )
        cursor = conn_sql.cursor()

        # Use the table name provided by the user
        create_table_from_df(cursor, table_name, valid_rows)
        print(f"Table '{table_name}' successfully created in SQL Server!")

        insert_data_from_df(cursor, table_name, valid_rows)
        print(f"Data successfully inserted into the table '{table_name}' in SQL Server!")

        cursor.commit()
        cursor.close()
        conn_sql.close()

        return templates.TemplateResponse("index.html", {"request": request,
                                                         "success_message": "File processed and data inserted successfully."})

    except db.Error as ex:
        return templates.TemplateResponse("index.html",
                                          {"request": request, "error": f"Error connecting to SQL Server: {ex}"})

    except Exception as ex:
        return templates.TemplateResponse("index.html",
                                          {"request": request, "error": f"Error processing the file: {ex}"})


if __name__ == "__main__":
    if not os.path.exists("static/uploads"):
        os.makedirs("static/uploads")

    try:
        uvicorn.run(app, host="localhost", port=8000)
    except KeyboardInterrupt:
        print("Server shutdown requested, stopping...")
