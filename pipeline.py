import datetime
import json
from argparse import ArgumentParser, Namespace
from typing import Dict, List, Optional

import prefect
import pyspark.sql.functions as F
import requests
from prefect import flow, get_run_logger, task
from prefect.filesystems import Azure as AzureBucket
from pyspark.sql import SparkSession

from mtaprefect.blocks import (
    AzureContainer,
    DatabricksConnectCluster,
    IcebergAzureHadoop,
)


def get_spark(configs: Dict[str, str]) -> SparkSession:
    builder = SparkSession.builder
    for key, val in configs.items():
        builder = builder.config(key, val)
    spark = builder.getOrCreate()
    return spark


@task
def get_bnt_data(
    start_date: datetime.date,
    stop_date: Optional[datetime.date] = None,
    chunksize: int = 100000,
) -> List[Dict]:
    _logger = get_run_logger()
    url = "https://data.ny.gov/resource/qzve-kjga.json"
    start_date_str = start_date.strftime("%Y-%m-%dT00:00:00.00")
    where_clause = f'date>="{start_date_str}"'

    if stop_date:
        stop_date_str = stop_date.strftime("%Y-%m-%dT00:00:00.00")
        where_clause = f'{where_clause} and date<"{stop_date_str}"'

    params = {"$limit": chunksize, "$offset": 0, "$where": where_clause}

    data: List[Dict] = []
    _logger.info(
        "Querying data from %s with start_date=%s and stop_date=%s",
        url,
        start_date,
        stop_date,
    )
    while True:
        res = requests.get(url, params=params)
        res.raise_for_status()

        chunk = res.json()
        _logger.info("Got chunk of %d records", len(chunk))

        data.extend(chunk)

        if len(chunk) < chunksize:
            break
        else:
            params["$offset"] = params["$offset"] + chunksize  # type: ignore

    _logger.info("Got %d records total", len(data))
    return data


@task
def load_to_azure_blob(
    data: List[Dict],
    container: AzureContainer,
    blob_name: str,
):
    _logger = get_run_logger()

    blob_data = json.dumps(data)
    _logger.info("Writing data to %s", container.url(path=blob_name, protocol="https"))
    container.get_blob_client(blob_name).upload_blob(blob_data, overwrite=True)
    _logger.info("Finished writing blob")


@task(retries=1, retry_delay_seconds=60)
def create_bnt_iceberg_table(
    spark_configs: Dict[str, str],
    table_name: str,
):
    spark = get_spark(spark_configs)

    create_query = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        plaza_id string,
        date date,
        hour string,
        direction string,
        vehicles_e_zpass int,
        vehicles_vtoll int
    )
    USING iceberg
    PARTITIONED BY (years(date))
    """
    spark.sql(create_query).collect()


@task(retries=1, retry_delay_seconds=60)
def append_bnt_blob_to_iceberg(
    spark_configs: Dict[str, str],
    table_name: str,
    blob_uri: str,
):
    spark = get_spark(spark_configs)

    df = spark.read.json(blob_uri)

    df = (
        df.withColumn("date", F.to_date(df["date"]))
        .withColumn("vehicles_e_zpass", df["vehicles_e_zpass"].cast("int"))
        .withColumn("vehicles_vtoll", df["vehicles_vtoll"].cast("int"))
    )

    df.writeTo(table_name).append()


@task
def get_bnt_date_params(
    start_date: Optional[datetime.date],
    stop_date: Optional[datetime.date],
):
    if start_date is None or stop_date is None:
        base_date = prefect.context.get("scheduled_start_time")
        stop_date = base_date - datetime.timedelta(days=1)
        start_date = base_date - datetime.timedelta(days=8)
    return start_date, stop_date


@flow
def pipeline(
    start_date: Optional[datetime.date] = None,
    stop_date: Optional[datetime.date] = None,
):
    mtademo_bucket = AzureContainer.load("mtademo-container")
    iceberg = IcebergAzureHadoop.load("mtademo-iceberg")
    databricks_cluster = DatabricksConnectCluster.load("mtademo-singlenode-databricks")

    blob_name = "stage/bnt_traffic_hourly.json"
    table_name = iceberg.get_table_name(zone="bronze", table="bnt_traffic_hourly")

    spark_configs = {**iceberg.spark_configs(), **databricks_cluster.spark_configs()}

    start_date, stop_date = get_bnt_date_params.submit(start_date, stop_date)
    data = get_bnt_data.submit(
        start_date,
        stop_date,
    )

    table_created = create_bnt_iceberg_table.submit(
        spark_configs,
        table_name,
    )

    blob_loaded = load_to_azure_blob.submit(
        data,
        mtademo_bucket,
        blob_name,
        wait_for=[data],
    )

    append_bnt_blob_to_iceberg.submit(
        spark_configs,
        table_name,
        mtademo_bucket.url(blob_name),
        wait_for=[table_created, blob_loaded],
    )


def main(args: Namespace):
    print("Executing pipeline")
    pipeline(
        start_date=args.start_date,
        stop_date=args.stop_date,
    )
    print("Pipeline complete")


def get_args() -> Namespace:
    parser = ArgumentParser()

    parser.add_argument(
        "--start-date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(),
        help="Starting date to pull data for",
        required=True,
    )

    parser.add_argument(
        "--stop-date",
        type=lambda d: datetime.datetime.strptime(d, "%Y-%m-%d").date(),
        help="Stop date to pull data for",
        required=False,
    )

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = get_args()
    main(args)
