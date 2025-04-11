from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pendulum

with DAG(
    dag_id='master_dag',
    schedule_interval='0 * * * *',  # Запуск каждые 15 минут
    start_date=pendulum.datetime(2025, 4, 2, tz="UTC"),  # Дата начала выполнения
    catchup=False,  # Не запускать пропущенные интервалы
    tags=['master'],  # Теги
    is_paused_upon_creation=True  # Даг создается в остановленном состоянии
) as dag:

    api_data_loader_dag = TriggerDagRunOperator(
        task_id='api_data_loader_dag',
        trigger_dag_id='api_data_loader_dag',  # имя дага, который ты хочешь запустить
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    stg_bonus_system_ranks_dag = TriggerDagRunOperator(
        task_id='stg_bonus_system_ranks_dag',
        trigger_dag_id='stg_bonus_system_ranks_dag',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    stg_order_system = TriggerDagRunOperator(
        task_id='stg_order_system',
        trigger_dag_id='stg_order_system',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    dds_dag = TriggerDagRunOperator(
        task_id='dds_dag',
        trigger_dag_id='dds_dag',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )

    cdm_dag = TriggerDagRunOperator(
        task_id='cdm_dag',
        trigger_dag_id='cdm_dag',
        wait_for_completion=True,
        reset_dag_run=True,
        poke_interval=30,
        allowed_states=["success"],
        failed_states=["failed", "skipped"]
    )


    [api_data_loader_dag, stg_bonus_system_ranks_dag, stg_order_system] >> dds_dag >> cdm_dag
