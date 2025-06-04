from enum import Enum

from pydantic import BaseModel, ConfigDict

from fastapi import UploadFile, Form


class ProcessingType(str, Enum):
    FILTER_DATA = "Filter data"
    AGGREGATE_DATA = "Aggregate data"
    GENERATE_SUMMARY = "Generate summary"


class DatasetForm(BaseModel):
    file: UploadFile
    processing_type: ProcessingType
    condition: str = Form(max_length=100)
    is_sample_data: bool
    session_id: str

    model_config = ConfigDict(extra="forbid")
