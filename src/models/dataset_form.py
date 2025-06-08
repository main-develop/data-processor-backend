from enum import Enum

from pydantic import BaseModel, ConfigDict
from typing import Literal

from fastapi import Form, UploadFile


class ProcessingType(str, Enum):
    FILTER_DATA = "Filter data"
    AGGREGATE_DATA = "Aggregate data"
    GENERATE_SUMMARY = "Generate summary"


class AggregationFunction(str, Enum):
    SUM = "sum"
    MEAN = "mean"
    COUNT = "count"
    MIN = "min"
    MAX = "max"


class DatasetForm(BaseModel):
    is_sample_data: bool = False
    session_id: str

    model_config = ConfigDict(extra="forbid")


class AnalyzeDatasetForm(DatasetForm):
    file: UploadFile


class FilterDatasetForm(DatasetForm):
    processing_type: Literal[ProcessingType.FILTER_DATA]
    condition: str = Form(max_length=100)


class AggregateDatasetForm(DatasetForm):
    processing_type: Literal[ProcessingType.AGGREGATE_DATA]
    group_by_column: str
    aggregation_column: str
    aggregation_function: AggregationFunction


class SummaryDatasetForm(DatasetForm):
    processing_type: Literal[ProcessingType.GENERATE_SUMMARY]


GeneralDatasetForm = FilterDatasetForm | AggregateDatasetForm | SummaryDatasetForm
