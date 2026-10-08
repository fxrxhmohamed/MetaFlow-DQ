from pydantic import BaseModel, Field


class TripIngestRequest(BaseModel):
    years: list[int] = Field(default_factory=lambda: [2024, 2025, 2026])
    quarters: list[int] | None = None  # None = every published quarter of those years
    discover: bool = True  # re-read the data page so newly published quarters are included
    force: bool = False  # re-land even if the archive checksum is unchanged


class FileReport(BaseModel):
    file_name: str
    encoding: str
    row_count: int
    ragged_rows: int
    missing_columns: list[str]
    unexpected_columns: list[str]
    column_order_matches: bool
    sha256: str


class IngestResult(BaseModel):
    dataset: str
    key: str
    batch_id: str | None = None
    status: str  # landed | unchanged | failed
    rows: int = 0
    files: list[FileReport] = Field(default_factory=list)
    error: str | None = None
