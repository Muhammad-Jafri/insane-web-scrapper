from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, HttpUrl


class JobCreate(BaseModel):
    url: HttpUrl
    priority: int = 0


class BulkJobCreate(BaseModel):
    urls: list[HttpUrl]


class JobCreatedResponse(BaseModel):
    job_id: UUID
    status: str
    created_at: datetime


class BulkJobCreatedResponse(BaseModel):
    job_ids: list[UUID]
    enqueued: int
    rejected: int


class JobResponse(BaseModel):
    job_id: UUID
    url: str
    status: str
    retries: int
    result: dict[str, Any] | None
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None
