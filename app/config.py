from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    database_url: str
    redis_url: str
    s3_endpoint_url: str
    s3_bucket: str
    s3_access_key: str
    s3_secret_key: str

    worker_concurrency: int = 10
    max_concurrency_per_domain: int = 3
    queue_depth_limit: int = 10000
    brpop_timeout: int = 5
    fetch_timeout: int = 15
    metrics_port: int = 9090

    log_max_bytes: int = 10 * 1024 * 1024  # 10 MB
    log_backup_count: int = 5


settings = Settings()
