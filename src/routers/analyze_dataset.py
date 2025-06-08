import asyncio
import os
from pathlib import Path
from typing import Annotated, List

import dask.dataframe as dd
import pandas as pd
from fastapi import APIRouter, Form, HTTPException
from fastapi.responses import JSONResponse

from ..models.column_metadata import ColumnMetadata
from ..models.dataset_form import AnalyzeDatasetForm
from ..models.dataset_metadata import DatasetMetadata
from ..websocket_manager import websocket_send_progress

UPLOAD_DIR = Path("uploads")
UPLOAD_DIR.mkdir(exist_ok=True)

analyze_dataset_router = APIRouter()


@analyze_dataset_router.post("/analyze-dataset")
async def analyze_dataset(dataset_form: Annotated[AnalyzeDatasetForm, Form()]):
    file_size = dataset_form.file.size
    if file_size > 10 * 1024 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="File size must be less than 10GB")

    file_extension = os.path.splitext(dataset_form.file.filename)[1].lower()
    if file_extension not in {".csv", ".parquet", ".xls", ".xlsx", ".txt"}:
        raise HTTPException(status_code=400, detail="Invalid file type")

    file_path = UPLOAD_DIR / f"{dataset_form.session_id}{file_extension}"

    try:
        # File streaming with progress
        chunk_size = 1024 * 1024  # 1MB chunks
        total_size = dataset_form.file.size
        bytes_written = 0

        with file_path.open("wb") as buffer:
            while True:
                chunk = await dataset_form.file.read(chunk_size)
                if not chunk:
                    break
                buffer.write(chunk)
                bytes_written += len(chunk)
                progress = min((bytes_written / total_size) * 100, 100)
                await websocket_send_progress(
                    dataset_form.session_id, progress, "Uploading..."
                )
                await asyncio.sleep(0.05)

        await websocket_send_progress(
            dataset_form.session_id, 0, "Starting analysis..."
        )
        await asyncio.sleep(0.2)  # Ensure transitional message is visible

        # Read file
        if file_extension in {".csv", ".txt"}:
            df: dd.DataFrame = dd.read_csv(file_path, sample=250000)
        elif file_extension in {".parquet", ".pq"}:
            df = dd.read_parquet(file_path)
        elif file_extension in {".xls", ".xlsx"}:
            pandas_df = pd.read_excel(
                file_path, nrows=10000 if dataset_form.is_sample_data else None
            )
            df = dd.from_pandas(pandas_df, npartitions=4)

        columns: List[ColumnMetadata] = []
        sample_size = min(5, len(df))
        sample_data = (
            df.head(sample_size).to_dict(orient="records") if sample_size > 0 else []
        )

        # Simulate analysis progress
        total_steps = len(df.columns) + 2
        current_step = 0

        for column in df.columns:
            dtype = str(df[column].dtype.name)
            unique_count = df[column].nunique().compute()
            missing_percentage = df[column].isna().mean().compute()

            columns.append(
                ColumnMetadata(
                    name=column,
                    dtype=dtype,
                    unique=unique_count,
                    missing=missing_percentage,
                )
            )
            current_step += 1
            progress = (current_step / total_steps) * 100
            await websocket_send_progress(
                dataset_form.session_id, progress, "Analyzing columns..."
            )
            await asyncio.sleep(0.2)

        numeric_columns = [col for col in columns if col.dtype in ["int64", "float64"]]
        categorical_columns = [
            col for col in columns if col.dtype in ["object", "category", "string"]
        ]

        can_aggregate = len(numeric_columns) > 0 and len(categorical_columns) > 0
        can_filter = len(columns) > 1

        current_step += 1
        progress = (current_step / total_steps) * 100
        await websocket_send_progress(
            dataset_form.session_id, progress, "Finalizing metadata..."
        )
        await asyncio.sleep(0.2)

        dataset_metadata = DatasetMetadata(
            columns=columns,
            can_aggregate=can_aggregate,
            can_filter=can_filter,
            sample_data=sample_data,
        )

        await websocket_send_progress(dataset_form.session_id, 100, "Analysis Complete")
        return JSONResponse(content=dataset_metadata.dict())
    except Exception as e:
        await websocket_send_progress(dataset_form.session_id, 0, f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e)) from e
