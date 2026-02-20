from google.cloud import bigquery
from prefect import task
from prefect_gcp import GcsBucket, BigQueryWarehouse, GcpCredentials
from prefect.logging import get_run_logger
import pandas as pd
import os
import glob
import json
from dbt.cli.main import dbtRunner


def log(*args):
    logger = get_run_logger()
    return logger.info(*args)

class classe_gcp:
    def __init__(
        self,
        bucket_name,
        project,
        dataset_id,
        table_id,
        data_path_local,
        destination_directory,
    ):
        self.bucket_name = bucket_name
        self.project = project
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.data_path_local = data_path_local
        self.destination_directory = destination_directory
        self.storage_class = "STANDARD"
        self.location = "southamerica-east1"

    def __str__(self):
        return log(
            f"bucket_name: {self.bucket_name}\n project: {self.project}\n dataset_id: {self.dataset_id}\n table_id: {self.table_id}\n data_path_local: {self.data_path_local}\n destination_directory: {self.destination_directory}\n"
        )


def upload_files_in_directory(data_path_local = "output", destination_directory = "cgu/terceirizados"):
    bucket = GcsBucket.load("cgu")
    for root, _, files in os.walk(data_path_local):
        for file_name in files:
            source_file_name = os.path.join(root, file_name)

            relative_path = os.path.relpath(source_file_name, data_path_local)
            destination_file_name = os.path.join(destination_directory, relative_path).replace("\\", "/")
            
            bucket.upload_from_path(from_path=source_file_name, to_path=destination_file_name)
            



@task
def create_table_and_upload_data(
    bucket_name,
    project,
    dataset_id,
    table_id,
    data_path_local,
    destination_directory
):
    classe = classe_gcp(
        bucket_name,
        project,
        dataset_id,
        table_id,
        data_path_local,
        destination_directory,
    )
    log("Iniciando processo de upload e dump para o BigQuery...")
    classe.__str__()
    classe.upload_files_in_directory()
    classe.dump_to_bigquery()


@task
def invoke_dbt(model: str, table_id: str, commands: list[str] = ["run", "test"]) -> None:
    try:
        gcp_credentials = GcpCredentials.load("gcp-credentials-anp")

        # Obter o dicionário real
        credentials_dict = gcp_credentials.service_account_info.get_secret_value()
        
        with open("service-account.json", "w") as f:
            json.dump(credentials_dict, f)

        runner = dbtRunner()
        
        for command in commands:
            cli_args = [
                command,
                "--select",
                f"models/{model}/{table_id}.sql",
            ]
            
            log(f"Executando dbt: {cli_args}")
            runner.invoke(cli_args)
        
    finally:
        os.remove("service-account.json")