import json
import os

import requests
from azure.storage.blob import BlobClient
from prefect import flow, task


@task
def get_daily_ridership_from_api(url):
    response = requests.get(url)
    response.raise_for_status()
    return response.json()


@task
def load_data_to_blob(data, conn_str, container, blob_name):
    blob = BlobClient.from_connection_string(
        conn_str=conn_str,
        container_name=container,
        blob_name=blob_name,
    )

    blob_data = json.dumps(data)
    blob.upload_blob(blob_data, overwrite=True)


@flow
def daily_ridership_flow(url, conn_str, container):
    data = get_daily_ridership_from_api(url)
    load_data_to_blob(data, conn_str, container, "all_days.json")


if __name__ == "__main__":
    url = "https://data.ny.gov/resource/vxuj-8kew.json"
    conn_str = os.getenv("AZURE_CONN_STR")
    container = "daily-ridership"
    daily_ridership_flow(url, conn_str, container)
