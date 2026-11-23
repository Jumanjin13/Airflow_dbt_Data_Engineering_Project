import time
from datetime import timedelta
from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState


@dag
def orchestrate():

    @task(execution_timeout=timedelta(hours=1))
    def ingest_cdc():
        ws = WorkspaceClient()

        job_trigger = ws.jobs.run_now(job_id="849963093726427")

        poll_deadline = time.monotonic() + timedelta(minutes=55).total_seconds()
        while True:
            if time.monotonic() >= poll_deadline:
                raise TimeoutError("Databricks job did not finish within 55 minutes")

            job_run = ws.jobs.get_run(job_trigger.run_id)

            if job_run.state.life_cycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED, RunLifeCycleState.INTERNAL_ERROR]:
                if job_run.state.result_state == RunResultState.SUCCESS:
                    print("Job completed successfully!")
                    break 
                else:
                    raise Exception(f"Job failed with state: {job_run.state.result_state}")
                    
            time.sleep(5)  # Wait for 5 seconds before checking the job status again
        
        return "CDC Ingestion Completed"

    @task.bash
    def clean_target():
        return "rm -rf /opt/airflow/walmart_dbt_project/target && rm -rf /opt/airflow/walmart_dbt_project/logs"

    @task.bash
    def source_freshness():
        return "cd /opt/airflow/walmart_dbt_project && dbt source freshness"

    silver_technical = BashOperator(
        task_id="silver_technical",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select silver_t"
    )

    silver_technical_tests = BashOperator(
        task_id="silver_technical_tests",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt test --select silver_t"
    )

    silver_business = BashOperator(
        task_id="silver_business",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select silver_b"
    )

    silver_business_tests = BashOperator(
        task_id="silver_business_tests",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt test --select silver_b"
    )

    gold_ephemeral = BashOperator(
        task_id="gold_ephemeral",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select gold/ephemeral"
    )

    gold_dimensions = BashOperator(
        task_id="gold_dimensions",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt snapshot"
    )

    gold_facts = BashOperator(
        task_id="gold_facts",
        cwd="/opt/airflow/walmart_dbt_project",
        bash_command="dbt run --select gold/fact"
    )


    ingest_cdc() >> clean_target() >> source_freshness() >> silver_technical >> silver_technical_tests >> silver_business >> silver_business_tests >> gold_ephemeral >> gold_dimensions >> gold_facts


orchestrate_dag = orchestrate()