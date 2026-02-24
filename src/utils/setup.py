
from prefect import task
from prefect_gcp import GcsBucket, GcpCredentials
from prefect.logging import get_run_logger
import os
import json
from dbt.cli.main import dbtRunner
import duckdb
from dateutil.relativedelta import relativedelta
from typing import List
from dotenv import load_dotenv
import subprocess

def log(*args):
    logger = get_run_logger()
    return logger.info(*args)

@task
def upload_files_in_directory(data_path_local : str, destination_directory : str) -> None:
    bucket = GcsBucket.load("cgu")
    for root, _, files in os.walk(data_path_local):
        for file_name in files:
            source_file_name = os.path.join(root, file_name)
            log(f"Uploading {source_file_name}")
            relative_path = os.path.relpath(source_file_name, data_path_local)
            destination_file_name = os.path.join(destination_directory, relative_path).replace("\\", "/")
            
            bucket.upload_from_path(from_path=source_file_name,
                                    to_path=destination_file_name
                                    )


@task
def invoke_dbt(
                targets: List[str],
                commands: List[str]
                ) -> None:
    try:
        gcp_credentials = GcpCredentials.load("cgu-service-account")

        credentials_dict = gcp_credentials.service_account_info.get_secret_value()
        
        with open("service-account.json", "w") as f:
            json.dump(credentials_dict, f)
        
        load_dotenv(dotenv_path=".env")
        
        token = subprocess.check_output(
            ["gcloud", "auth", "print-access-token"],
            text=True
        ).strip()

        os.environ["GCS_TOKEN"] = token
        
        runner = dbtRunner()
        
        for command in commands:
            for target in targets:
                cli_args = [
                    command,
                    "--target",
                    target,
                    "--select",
                    target,
                ]
            
                log(f"executing dbt: {cli_args}")
                runner.invoke(cli_args)
        
    finally:
        os.remove("service-account.json")


def max_date_duckdb(duckdb_path: str = 'terceirizados-bronze.duckdb') -> str:

    con = duckdb.connect(duckdb_path)

    max_date = con.execute("""
        SELECT max(make_date(
        CAST(Ano_Carga AS INT),
        CAST(Num_Mes_Carga AS INT),
        1
    ))
    FROM br_cgu_terceirizados.bronze""").fetchone()[0]
    
    log(f"Max date in DuckDB: {str(max_date)}")
    log(f"Max date after adding 4 months: {str(max_date + relativedelta(months=4))}")
    
    max_date_after_four_month = str(max_date + relativedelta(months=4))[0:7].replace('-', '')

    return max_date_after_four_month