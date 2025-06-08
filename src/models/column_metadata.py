from pydantic import BaseModel, ConfigDict


class ColumnMetadata(BaseModel):
    name: str
    dtype: str
    unique: int
    missing: float

    model_config = ConfigDict(extra="forbid")
