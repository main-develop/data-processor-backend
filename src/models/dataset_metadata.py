from typing import Any, Dict, List

from pydantic import BaseModel, ConfigDict

from .column_metadata import ColumnMetadata


class DatasetMetadata(BaseModel):
    columns: List[ColumnMetadata]
    can_aggregate: bool
    can_filter: bool
    sample_data: List[Dict[str, Any]]

    model_config = ConfigDict(extra="forbid")
