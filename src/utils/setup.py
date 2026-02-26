
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
def upload_files_in_directory(data_path_local: str, destination_directory: List[str]) -> None:
    bucket = GcsBucket.load("cgu-bucket")

    for file_name in os.listdir(data_path_local):
        source_file_name = os.path.join(data_path_local, file_name)

        if not os.path.isfile(source_file_name):
            log(f"Skipping {source_file_name} because it is not a file.")
            continue

        destination = None

        for folder in destination_directory:
            if folder in file_name:
                destination = folder
                break

        if destination is None:
            log(f"Skipping {file_name}")
            continue

        destination_file_name = os.path.join(destination, file_name)

        log(f"Uploading {source_file_name} to {destination_file_name}")

        bucket.upload_from_path(
            from_path=source_file_name,
            to_path=destination_file_name
        )


@task
def invoke_dbt(
                targets: List[str],
                ) -> None:
    try:
        gcp_credentials = GcpCredentials.load("cgu-service-account")

        credentials_dict = gcp_credentials.service_account_info.get_secret_value()
        
        with open("service-account.json", "w") as f:
            json.dump(credentials_dict, f)
        
        load_dotenv(dotenv_path=".env")
        
        runner = dbtRunner()
        
        for target in targets:
            cli_args_run = [
                "run",
                "--target",
                target,
                "--select",
                target,
            ]
            
            log(f"executing dbt run : {cli_args_run}")
            runner.invoke(cli_args_run)
            
        cli_args_test = [
            'test',
            "--target",
            "silver",
            "--select",
            "silver",
        ]
            
        log(f"executing dbt test : {cli_args_test}")
        runner.invoke(cli_args_test)
        
    finally:
        os.remove("service-account.json")


def max_date_duckdb(file_parquet: str = "gs://br-cgu-terceirizados/terceirizados/*.parquet") -> str:

    # https://medium.com/@404c3s4r/subprocess-no-python-937a3c3bd518
    result = subprocess.run(['gcloud', 'auth', 'application-default', 'print-access-token'], 
                            capture_output=True, text=True)

    access_token = result.stdout.strip()

    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("DROP SECRET IF EXISTS gcs_secret")
    con.execute(f"CREATE SECRET gcs_secret (TYPE GCS, bearer_token '{access_token}')")

    result = con.execute(f"""
                        SELECT MAX(
                            make_date(
                                CAST(Ano_Carga AS INT),
                                CAST(Num_Mes_Carga AS INT),
                                1
                            )
                        ) as date
                        FROM read_parquet({file_parquet})
                        """).fetchone()[0]

    log(f"Max date in DuckDB: {str(result)}")
    log(f"Max date after adding 4 months: {str(result + relativedelta(months=4))}")

    result_after_four_month = str(result + relativedelta(months=4))[0:7].replace('-', '')
    
    return result_after_four_month