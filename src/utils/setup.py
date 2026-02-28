
from prefect import task
from prefect_gcp import GcsBucket
from prefect.logging import get_run_logger
import os
from dbt.cli.main import dbtRunner
import duckdb
from dateutil.relativedelta import relativedelta
from typing import List
from dotenv import load_dotenv
import google.auth
import google.auth.transport.requests
from google.oauth2 import service_account
import requests

def get_gcs_token():
    """
    Retrieve a valid GCS authentication token from a service account.
    This function loads credentials from a service account JSON file, refreshes
    the token to ensure validity, and returns the access token for authenticating
    requests to Google Cloud Storage and other GCP services.
    Returns:
        str: A valid OAuth 2.0 access token for GCP authentication.
    Raises:
        FileNotFoundError: If the service-account.json file is not found.
        ValueError: If the service account JSON file is invalid or malformed.
        google.auth.exceptions.RefreshError: If the token refresh fails.
    """
    creds = service_account.Credentials.from_service_account_file(
        "service-account.json",
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    
    session = requests.Session()
    auth_req = google.auth.transport.requests.Request(session=session)
    
    creds.refresh(auth_req)
    
    return creds.token

def log(*args):
    logger = get_run_logger()
    return logger.info(*args)

@task
def upload_files_in_directory(data_path_local: str, destination_directory: List[str]) -> None:
    """
    Upload files from a local directory to a Google Cloud Storage bucket based on folder matching.

    This function iterates through files in a local directory and uploads them to a GCS bucket.
    Each file is uploaded to a destination folder determined by matching the file name with
    entries in the destination_directory list.

    Args:
        data_path_local (str): The local directory path containing files to upload.
        destination_directory (List[str]): A list of destination folder names. Each file name
            is checked against these folder names, and if a match is found, the file is uploaded
            to that folder in the GCS bucket.

    Returns:
        None

    Raises:
        Logs a message and skips files that:
        - Are not files (e.g., directories)
        - Don't match any folder in destination_directory

    Note:
        - Uses the "cgu-bucket" GCS bucket for uploads
        - Requires the GcsBucket class to be properly initialized
        - Non-file items and unmatched files are skipped with logging
    """
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
def invoke_dbt() -> None:
    
    load_dotenv(dotenv_path=".env")

    token = get_gcs_token()


    os.environ["GCS_TOKEN"] = token
    
    runner = dbtRunner()
    log("Start Run Bronze!!!")
    
    runner.invoke([
        "run",
        "--target",
        "bronze",
        "--select",
        "bronze",
    ])
    
    log("Start Run Silver!!!")
    runner.invoke([
        "run",
        "--target",
        "silver",
        "--select",
        "silver",
    ])
    
    log("Start Teste Silver!!!")
    result = runner.invoke([
        "test",
        "--target",
        "silver",
        "--select",
        "silver",
    ])

    if result.success:
        log("Start Run Gold!!!")
        runner.invoke([
            "run",
            "--target",
            "gold",
            "--select",
            "gold",
        ])
    else:
        raise Exception("Silver tests failed. Aborting gold execution.")


def max_date_duckdb(file_parquet: str) -> str:
    
    con = duckdb.connect()
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("DROP SECRET IF EXISTS gcs_secret")
    con.execute(f"CREATE SECRET gcs_secret (TYPE GCS, bearer_token '{get_gcs_token()}')")

    result = con.execute(f"""
                        SELECT MAX(
                            make_date(
                                CAST(Ano_Carga AS INT),
                                CAST(Num_Mes_Carga AS INT),
                                1
                            )
                        ) as date
                        FROM read_parquet('{file_parquet}')
                        """).fetchone()[0]

    log(f"Max date in DuckDB: {str(result)}")
    log(f"Max date after adding 4 months: {str(result + relativedelta(months=4))}")

    result_after_four_month = str(result + relativedelta(months=4))[0:7].replace('-', '')
    
    return result_after_four_month


def build_filenames(yyyymm: str):
    meses = {
        1: "janeiro",
        5: "maio",
        9: "setembro",

    }

    if yyyymm == '202601':
        year = int(yyyymm[:4])
        month = int(yyyymm[4:6])

        date = f"{year}{month:02d}"
        text = f"{meses[month]}"
        return [date, text]

    else:
        return yyyymm