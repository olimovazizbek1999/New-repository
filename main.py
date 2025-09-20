import os
from dotenv import load_dotenv
from fastapi import FastAPI, UploadFile, Form
from fastapi.responses import HTMLResponse
from services import gcs, processing

# Load environment variables from .env
load_dotenv()

# FastAPI app
app = FastAPI()

# Read env variables
GCS_BUCKET = os.getenv("GCS_BUCKET")


@app.get("/", response_class=HTMLResponse)
async def index():
    """Serve a simple HTML upload form."""
    return """
    <html>
        <body>
            <h2>Upload CSV File</h2>
            <form action="/upload" method="post" enctype="multipart/form-data">
                <input type="file" name="file" />
                <input type="text" name="output_file" placeholder="Output filename" />
                <button type="submit">Upload</button>
            </form>
        </body>
    </html>
    """


@app.post("/upload")
async def upload_csv(file: UploadFile, output_file: str = Form(...)):
    """Upload CSV to GCS, process, and save results."""
    input_path = f"input/{file.filename}"
    output_path = f"output/{output_file}"

    # Upload to GCS
    gcs.upload_fileobj(GCS_BUCKET, input_path, file.file)

    # Process file (e.g., clean/transform)
    processing.process_csv(GCS_BUCKET, input_path, output_path)

    return {"message": "File processed successfully", "output_file": output_file}
