from prefect import flow
from src.utils.setup import upload_files_in_directory, invoke_dbt
from src.pipelines.tasks import (
    check_for_update_and_download,
    ingest_and_partition,
    download_all_data,
    
)

@flow(
    log_prints=False,
    name='CGU Data Pipeline'
)
def flow_cgu(
    load_to_data :  bool = False,
    date: str | None = None,
    ):
    
    if load_to_data:
        download_all_data()
        
        ingest_and_partition(input = 'input', output='output')
        
    
    else:
        if not check_for_update_and_download(['.csv','.xlsx'], date = date):
            print("Nenhum arquivo novo encontrado. Encerrando o processo.")
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
        load_to_data=True,
        date=None
    )