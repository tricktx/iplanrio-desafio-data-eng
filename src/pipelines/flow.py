from prefect import flow
from src.utils.setup import upload_files_in_directory, invoke_dbt
from src.pipelines.tasks import (
    check_for_update,
    download_data,
    ingest_and_partition,
)

@flow(
    log_prints=False,
    name='CGU Data Pipeline'
)
def flow_cgu() -> None:
    
    # if not check_for_update(['.csv','.xlsx']):
    #     print("Nenhum arquivo novo encontrado. Encerrando o processo.")
    #     return
    
    # download_data(
    #     ['.csv',
    #     '.xlsx']
    #     )
    
    # ingest_and_partition(
    #     input = 'input',
    #     output = 'output'
    # )
    
    # upload_files_in_directory(data_path_local = "output", destination_directory=["terceirizados/"])
    
    invoke_dbt(
        targets = ["bronze", "silver", "gold"],
    )
    
    upload_files_in_directory(data_path_local = "duckdb", destination_directory=["bronze", "silver", "gold"])
    