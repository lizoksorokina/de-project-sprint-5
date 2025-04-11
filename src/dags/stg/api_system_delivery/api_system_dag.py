import logging
import pendulum
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.models import Variable


log = logging.getLogger(__name__)


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 4, 2, tz="UTC"),  # Дата начала выполнения
    catchup=False,  # Не запускать пропущенные интервалы
    tags=['api_data_loader', 'couriers', 'example'],  # Теги
    is_paused_upon_creation=True  # Даг создается в остановленном состоянии
)
def api_data_loader_dag():

    @task(task_id="fetch_and_store_data")
    def fetch_and_store_data(resource: str):
        nickname = 'XXXXX'
        cohort = 'XXXXX'
        api_token = '25c27781-8fde-4b30-a22e-524044a7580f'
        couriers_url = f"https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/{resource}"

        headers = {
            "X-Nickname": nickname,
            "X-Cohort": cohort,
            "X-API-KEY": api_token
        }

        pg_hook = PostgresHook(postgres_conn_id='PG_WAREHOUSE_CONNECTION')
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        limit = 50
        offset = 0

        while True:
            # Параметры для пагинации
            params = {"sort_field": "name", "sort_direction": "asc", "limit": limit, "offset": offset}

            response = requests.get(couriers_url, headers=headers, params=params)

            if response.status_code != 200:
                log.error(f"Ошибка: {response.status_code}, {response.text}")
                break

            data = response.json()
            if not data:  # Если данных больше нет, выходим из цикла
                break

            for record in data:
                cur.execute(f"""
                    INSERT INTO stg.{resource} (data, update_ts) 
                    VALUES (%s, NOW()) 
                    ON CONFLICT (data) DO UPDATE SET update_ts = NOW();
                """, (json.dumps(record),))

            conn.commit()
            offset += limit

        query = """
            INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings) 
            VALUES (%s, %s)
            ON CONFLICT (workflow_key) DO UPDATE 
            SET workflow_settings = excluded.workflow_settings;
        """
        workflow_key = f"{resource}_origin_to_stg_workflow"
        workflow_settings = json.dumps({"last_loaded_offset": offset})
        cur.execute(query, (workflow_key, workflow_settings))
        conn.commit()

        cur.close()
        conn.close()

    # Вызов задачи
    couriers = fetch_and_store_data("couriers")
    deliveries = fetch_and_store_data("deliveries")
    [couriers, deliveries]



stg_dag=api_data_loader_dag()