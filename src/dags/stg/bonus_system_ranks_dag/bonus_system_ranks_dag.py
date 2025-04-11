import logging

import pendulum
from airflow.decorators import dag, task
from examples.stg.bonus_system_ranks_dag.ranks_loader import RankLoader
from examples.stg.bonus_system_ranks_dag.users_loader import UserLoader
from examples.stg.bonus_system_ranks_dag.events_loader import EventLoader


from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'stg', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)

def stg_bonus_system_ranks_dag():
    # Подключения к базам данных
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")
    origin_pg_connect = ConnectionBuilder.pg_conn("PG_ORIGIN_BONUS_SYSTEM_CONNECTION")

    # Таск загрузки рангов
    @task(task_id="ranks_load")
    def load_ranks():
        rank_loader = RankLoader(origin_pg_connect, dwh_pg_connect, log)
        rank_loader.load_ranks()

    # Таск загрузки пользователей
    @task(task_id="users_load")
    def load_users():
        user_loader = UserLoader(origin_pg_connect, dwh_pg_connect, log)
        user_loader.load_users()

    # Таск загрузки событий
    @task(task_id="events_load")
    def load_events():
        event_loader = EventLoader(origin_pg_connect, dwh_pg_connect, log)
        event_loader.load_events()

    # Определяем порядок выполнения
    ranks_task = load_ranks()
    users_task = load_users()
    events_task = load_events()

    ranks_task >> users_task >> events_task # users_load выполнится после ranks_load


stg_bonus_system_dag = stg_bonus_system_ranks_dag()