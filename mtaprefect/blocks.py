"""
Custom Blocks
"""
from typing import Optional

from azure.storage.blob import BlobClient
from prefect.blocks.core import Block
from prefect.filesystems import Azure
from prefect_databricks import DatabricksCredentials


class AzureContainer(Block):
    container: str
    credentials: Azure

    def url(self, path: Optional[str] = None, protocol: Optional[str] = None) -> str:
        if protocol in (None, "abfss"):
            url = f"abfss://{self.container}@{self.credentials.azure_storage_account_name.get_secret_value()}.dfs.core.windows.net"
        elif protocol in ("https", "http"):
            url = f"{protocol}://{self.credentials.azure_storage_account_name.get_secret_value()}.blob.core.windows.net"

        if path is not None:
            url = "/".join([url, path])

        return url

    def get_blob_client(self, blob_name):
        blob = BlobClient(
            account_url=self.url(protocol="https"),
            container_name=self.container,
            blob_name=blob_name,
            credential=self.credentials.azure_storage_account_key.get_secret_value(),
        )
        return blob


class IcebergAzureHadoop(Block):
    catalog_name: str
    warehouse_location: str
    container: AzureContainer

    def spark_configs(self):
        return {
            f"fs.azure.account.key.{self.container.credentials.azure_storage_account_name.get_secret_value()}.dfs.core.windows.net": self.container.credentials.azure_storage_account_key.get_secret_value(),
            f"spark.sql.catalog.{self.catalog_name}": "org.apache.iceberg.spark.SparkCatalog",
            f"spark.sql.catalog.{self.catalog_name}.type": "hadoop",
            f"spark.sql.catalog.{self.catalog_name}.warehouse": self.container.url(
                self.warehouse_location
            ),
        }

    def get_table_name(self, table: str, zone: Optional[str] = None) -> str:
        fqn = self.catalog_name

        if zone is not None:
            fqn = f"{fqn}.{zone}"

        return f"{fqn}.{table}"


class DatabricksConnectCluster(Block):
    cluster_id: str
    credentials: DatabricksCredentials

    def spark_configs(self):
        return {
            "spark.databricks.service.address": f"https://{self.credentials.databricks_instance}",
            "spark.databricks.service.token": self.credentials.token.get_secret_value(),
            "spark.databricks.service.clusterId": self.cluster_id,
        }
