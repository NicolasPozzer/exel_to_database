from fastapi import FastAPI, File, UploadFile, Form
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from starlette.requests import Request
import pandas as pd
import pyodbc as db
import os

# Importa las funciones necesarias desde tu módulo
from src.components.data_validate import create_table_from_df, insert_data_from_df, save_invalid_rows, validate_dataframe

app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Directorio para subir y servir archivos estáticos
app.mount("/static", StaticFiles(directory="static"), name="static")

# Página principal
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

# Ruta para manejar la carga del archivo
@app.post("/upload")
async def upload_file(request: Request, file: UploadFile = File(...), table_name: str = Form(...)):
    if file.filename == '':
        return templates.TemplateResponse("index.html", {"request": request, "error": "No file selected"})

    file_path = os.path.join("static/uploads", file.filename)
    with open(file_path, "wb") as buffer:
        buffer.write(file.file.read())

    df = pd.read_excel(file_path)

    valid_rows, invalid_rows, first_row_types = validate_dataframe(df)

    if not invalid_rows.empty:
        error_file_path = os.path.join("static/uploads", "invalid_rows.xlsx")
        save_invalid_rows(invalid_rows, error_file_path, df, first_row_types)
        return templates.TemplateResponse("index.html", {"request": request, "error": "Se encontraron filas inválidas.", "error_file": f"/static/uploads/invalid_rows.xlsx"})

    try:
        conn_sql = db.connect(
            driver="SQL Server",
            server="DESKTOP-NICO",
            database="exel_to_db",
            user="",
            password=""
        )
        cursor = conn_sql.cursor()

        # Utiliza el nombre de la tabla proporcionado por el usuario
        create_table_from_df(cursor, table_name, valid_rows)
        print(f"Table '{table_name}' successfully created in SQL Server!")

        insert_data_from_df(cursor, table_name, valid_rows)
        print(f"Data successfully inserted into the table '{table_name}' in SQL Server!")

        cursor.commit()
        cursor.close()
        conn_sql.close()
        return templates.TemplateResponse("index.html", {"request": request, "error": "Archivo procesado y datos insertados correctamente."})

    except db.Error as ex:
        return templates.TemplateResponse("index.html", {"request": request, "error": f"Error connecting to SQL Server: {ex}"})

    except Exception as ex:
        return templates.TemplateResponse("index.html", {"request": request, "error": f"Error processing the file: {ex}"})

if __name__ == "__main__":
    if not os.path.exists("static/uploads"):
        os.makedirs("static/uploads")
    import uvicorn
    uvicorn.run(app, host="localhost", port=8000)
