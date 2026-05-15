FROM ghcr.io/astral-sh/uv:python3.11-bookworm-slim

WORKDIR /app

# Install deps first — this layer is cached until pyproject.toml or uv.lock changes
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

# Copy source
COPY app/ ./app/
COPY migrations/ ./migrations/
COPY alembic.ini ./
COPY main.py worker.py ./

ENV PATH="/app/.venv/bin:$PATH"

# Default: API. Worker overrides this in docker-compose.
CMD ["uvicorn", "app.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
