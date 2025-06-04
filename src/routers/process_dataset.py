from pathlib import Path
import shutil
import os
import time

from ..models.dataset_form import DatasetForm

from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import JSONResponse
from typing import Annotated

# Directory to temporarily store uploaded files
UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

process_dataset_router = APIRouter()


@process_dataset_router.post("/process-dataset")
async def process_dataset(dataset_form: Annotated[DatasetForm, Form()]):
    valid_extensions = {".csv", ".parquet", ".xls", ".xlsx", ".txt"}
    file_extension = os.path.splitext(dataset_form.file.filename)[1].lower()

    if file_extension not in valid_extensions:
        raise HTTPException(status_code=400, detail="Invalid file type")

    file_size = dataset_form.file.size
    if file_size > 10 * 1024 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File size must be less than 10GB")
    
    file_path = UPLOAD_DIR / f"{dataset_form.session_id}{file_extension}"

    try:
        with file_path.open("wb") as buffer:
            shutil.copyfileobj(dataset_form.file.file, buffer)
        
        # Return a mock graph data response
        graph_data = [
            {"name": "Electronics", "value": 500000, "fill": "#1f77b4"},
            {"name": "Clothing", "value": 300000, "fill": "#ff7f0e"},
            {"name": "Books", "value": 200000, "fill": "#2ca02c"},
            {"name": "Toys", "value": 150000, "fill": "#d62728"},
        ]

        # Could store in a database for tracking
        print(f"Processing request from session: {dataset_form.session_id}")

        return JSONResponse(content={"graph_data": graph_data})
    finally:
        time.sleep(5)
        if file_path.exists():
            file_path.unlink()
