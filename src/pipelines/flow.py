from prefect import flow
from src.utils.setup import upload_files_in_directory

@flow(
    log_prints=False
)
def flow_cgu(
) -> None:


    
    upload_files_in_directory()
    

if __name__ == "__main__":
    flow_cgu()