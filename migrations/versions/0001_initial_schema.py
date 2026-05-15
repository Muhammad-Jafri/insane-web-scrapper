"""initial schema

Revision ID: 0001
Revises:
Create Date: 2026-05-16

"""

from typing import Sequence, Union

from alembic import op

revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE jobs (
            id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            url             TEXT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'PENDING',
            priority        INTEGER NOT NULL DEFAULT 0,
            retries         INTEGER NOT NULL DEFAULT 0,
            max_retries     INTEGER NOT NULL DEFAULT 3,
            next_retry_at   TIMESTAMPTZ,
            error_message   TEXT,
            raw_html_key    TEXT,
            result          JSONB,
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
            started_at      TIMESTAMPTZ,
            completed_at    TIMESTAMPTZ,
            worker_id       TEXT
        )
    """)

    op.execute("""
        CREATE INDEX idx_jobs_status ON jobs(status)
    """)

    op.execute("""
        CREATE INDEX idx_jobs_next_retry_at ON jobs(next_retry_at)
        WHERE status = 'PENDING'
    """)

    op.execute("""
        CREATE TABLE job_events (
            id          BIGSERIAL PRIMARY KEY,
            job_id      UUID NOT NULL REFERENCES jobs(id),
            event       TEXT NOT NULL,
            worker_id   TEXT,
            message     TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)

    op.execute("""
        CREATE INDEX idx_job_events_job_id ON job_events(job_id)
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS job_events")
    op.execute("DROP TABLE IF EXISTS jobs")
