import os
import tempfile
from io import StringIO

import pandas as pd
from google.cloud import storage


def upload_df_to_gcs(df: pd.DataFrame, bucket_name: str, blob_path: str, service_key_path="gcp_service_key.json"):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_path)

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')

    print(f"Cleaned DataFrame uploaded to gs://{bucket_name}/{blob_path}")


def setup_google_credentials():
    credentials_json = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
    if not credentials_json:
        raise EnvironmentError("GOOGLE_APPLICATION_CREDENTIALS is not set")

    service_account_path = "/tmp/google_credentials.json"
    with open(service_account_path, "w") as f:
        f.write(credentials_json)

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = service_account_path


def process_file(filename: str) -> str:
    bucket_name = os.environ["GCS_BUCKET_NAME"]
    setup_google_credentials()

    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(filename)

    with tempfile.NamedTemporaryFile(delete=False) as tmp:
        blob.download_to_filename(tmp.name)
        df = pd.read_csv(tmp.name)

    df = df.drop_duplicates()
    if 'title' in df.columns:
        df = df.drop_duplicates(subset='title', keep='first')

    cleaned_filename = filename.replace("feed_stories", "feed_stories_cleaned")

    upload_df_to_gcs(
        df=df,
        bucket_name=bucket_name,
        blob_path=cleaned_filename,
        service_key_path=os.environ.get("SERVICE_KEY_PATH")
    )


if __name__ == "__main__":
    filename = os.environ.get("SCRAPER_OUTPUT_FILENAME")
    cleaned_filename = process_file(filename)
    print(f"CLEANED_FILENAME={cleaned_filename}")
