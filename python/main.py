import asyncio
from io import BytesIO
from aiobotocore.session import get_session, AioSession
import polars as pl

BUCKET_NAME = "testing-async"
FILE_KEY = "revised.parquet"


async def read_parquet_async(
    session: AioSession, bucket: str, file_key: str
) -> pl.DataFrame:
    async with session.create_client("s3") as client:
        print(f"Reading from {bucket}/{file_key}")
        response = await client.get_object(Bucket=bucket, Key=file_key)
        async with response["Body"] as stream:
            data = await stream.read()
            return pl.read_parquet(BytesIO(data))


async def write_csv_async(
    session: AioSession, df: pl.DataFrame, bucket: str, file_key: str
) -> pl.DataFrame:
    data = df.write_csv(None)
    async with session.create_client("s3") as client:
        print(f"Writing to {bucket}/{file_key}")
        return await client.put_object(Bucket=bucket, Key=file_key, Body=data)


def do_sync_work(df: pl.DataFrame) -> pl.DataFrame:
    # Just sum the value of all goods by day.
    # Note: We aren't doing anything with this as we still want a reasonable-sized upload
    return df.groupby("time_ref").agg(pl.col("value").sum())


async def download_and_reupload(
    session: AioSession, bucket: str, file_key: str, upload_file_key: str
):
    df = await read_parquet_async(session, bucket, file_key)
    do_sync_work(df)
    await write_csv_async(session, df, bucket, upload_file_key)


async def main():
    session = get_session()
    tasks = [
        download_and_reupload(session, BUCKET_NAME, FILE_KEY, f"{i}-FILE_KEY")
        for i in range(200)
    ]
    res = await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
