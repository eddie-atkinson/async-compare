import { GetObjectCommand, PutObjectCommand, S3Client } from "@aws-sdk/client-s3";
import pl from 'nodejs-polars';
import { Buffer } from 'node:buffer';


const client = new S3Client({});
const BUCKET_NAME = "testing-async"
const FILE_KEY = "revised.parquet"

const readParquet = async () => {
  console.log(`Reading from ${BUCKET_NAME}/${FILE_KEY}`);
  const res = await client.send(new GetObjectCommand({ Bucket: BUCKET_NAME, Key: FILE_KEY }));
  const data = Buffer.concat(await res.Body.toArray())
  return pl.readParquet(data);
};

const doSyncWork = (df: pl.DataFrame) => {
  return df.groupBy("time_ref").agg(pl.col("value").sum())
}

const writeParquet = async (df: pl.DataFrame, fileKey: string) => {
  console.log(`Writing to ${BUCKET_NAME}/${fileKey}`);
  await client.send(new PutObjectCommand({
    Bucket: BUCKET_NAME,
    Key: fileKey,
    Body: df.writeCSV()
  }))
};


const range = (n: number) => {
  return [...Array(n).keys()];
}

const main = async () => {
  const promises = range(200).map(async (i) => {
    const df = await readParquet();
    doSyncWork(df);
    await writeParquet(df, `${i}-${FILE_KEY}`)

  })
  await Promise.all(promises)
}

main().then(() => { })
