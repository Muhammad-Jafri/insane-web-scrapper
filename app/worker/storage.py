import asyncio
from uuid import UUID

import boto3
from botocore.exceptions import ClientError

from app.config import settings


def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=settings.s3_endpoint_url,
        aws_access_key_id=settings.s3_access_key,
        aws_secret_access_key=settings.s3_secret_key,
    )


async def ensure_bucket(s3_client) -> None:
    try:
        await asyncio.to_thread(s3_client.head_bucket, Bucket=settings.s3_bucket)
    except ClientError as e:
        if e.response["Error"]["Code"] in ("404", "NoSuchBucket"):
            await asyncio.to_thread(s3_client.create_bucket, Bucket=settings.s3_bucket)
        else:
            raise


async def upload_html(s3_client, html: str, job_id: UUID) -> str:
    key = f"html/{job_id}.html"
    await asyncio.to_thread(
        s3_client.put_object,
        Bucket=settings.s3_bucket,
        Key=key,
        Body=html.encode("utf-8"),
        ContentType="text/html; charset=utf-8",
    )
    return key
