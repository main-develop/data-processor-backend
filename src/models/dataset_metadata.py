from pydantic import BaseModel, ConfigDict
from typing import List, Dict, Any

from .column_metadata import ColumnMetadata


class DatasetMetadata(BaseModel):
    columns: List[ColumnMetadata]
    can_aggregate: bool
    can_filter: bool
    sample_data: List[Dict[str, Any]]
