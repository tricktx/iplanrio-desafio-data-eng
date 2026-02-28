from prefect import flow
from src.utils.setup import upload_files_in_directory, invoke_dbt, log
from src.pipelines.tasks import (
    check_for_update_and_download,
    ingest_and_partition,
    download_all_data,
    
)

@flow(
    name='CGU Data Pipeline'
)
def flow_cgu(
    load_to_data :  bool = False,
    date: str | None = None,
) -> None:
    """
    Execute the CGU (Controladoria-Geral da União) data pipeline workflow.
    This function orchestrates the complete ETL process for CGU data, including:
    - Optional full data download and ingestion
    - Incremental data update checking and download
    - Data partitioning and ingestion
    - File upload to cloud storage
    - DBT transformation execution
    - Transformed data upload to storage layers (bronze, silver, gold)
    Args:
        load_to_data (bool, optional): If True, downloads all data from scratch and 
            performs a complete ingestion. If False, performs incremental updates only. 
            Defaults to False.
        date (str | None, optional): Date filter for incremental data checking in 
            'YYYY-MM' format. Only used when load_to_data is False. 
            Defaults to None.
    Returns:
        None: Returns early if no new files are found during incremental update 
            (when load_to_data is False).
    Raises:
        Logs a message and exits without processing if no new files are found 
        during incremental update check.
    """
    
    if load_to_data:
        download_all_data()
        
        ingest_and_partition(input = 'input', output='output')
        
    
    else:
        if not check_for_update_and_download(['.csv','.xlsx'], date = date):
            log("Nenhum arquivo novo encontrado. Encerrando o processo.")
            return
        
        ingest_and_partition(
            input = 'input',
            output = 'output'
        )

    upload_files_in_directory(data_path_local = "output", destination_directory=["terceirizados"])

    invoke_dbt()

    upload_files_in_directory(data_path_local = "duckdb", destination_directory=["bronze", "silver", "gold"])
        

if __name__ == "__main__":
    flow_cgu(
        load_to_data=False,
        date=None
    )