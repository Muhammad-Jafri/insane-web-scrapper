import asyncio
from uuid import UUID

import boto3
import httpx
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


async def upload_images(
    s3_client,
    http_client: httpx.AsyncClient,
    image_urls: list[str],
    job_id: UUID,
) -> list[str]:
    keys = []
    for i, url in enumerate(image_urls):
        try:
            response = await http_client.get(url, follow_redirects=True)
            if response.status_code != 200:
                continue
            content_type = (
                response.headers.get("content-type", "image/jpeg").split(";")[0].strip()
            )
            ext = content_type.split("/")[-1]
            key = f"images/{job_id}/{i}.{ext}"
            await asyncio.to_thread(
                s3_client.put_object,
                Bucket=settings.s3_bucket,
                Key=key,
                Body=response.content,
                ContentType=content_type,
            )
            keys.append(key)
        except Exception:
            continue
    return keys
