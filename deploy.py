from prefect.client.schemas.schedules import CronSchedule
from src.pipelines.flow import flow_cgu

if __name__ == "__main__":
    flow_cgu.deploy(
        name="deploy-cgu",
        work_pool_name="work-pool-cgu",
        image="cgu-pipeline:latest",
        job_variables={
            "image_pull_policy": "Never",
            "networks": ["iplanrio-desafio-data-eng_prefect_network"],},
        schedule=CronSchedule(
            cron="0 19 * * *",
            timezone="America/Sao_Paulo"
        ),
        build=False,
        version="1.0",
        push=False
        
    )