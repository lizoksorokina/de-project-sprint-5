import logging

import pendulum
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.stg.order_system_restaurants_dag.pg_saver import PgSaver
from examples.stg.order_system_restaurants_dag.ordersystem_loader import RestaurantLoader, UserLoader, OrderLoader
from examples.stg.order_system_restaurants_dag.ordersystem_reader import MongoReader
from lib import ConnectionBuilder, MongoConnect

log = logging.getLogger(__name__)


@dag(
    schedule_interval=None,  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'example', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def stg_order_system():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    # Получаем переменные из Airflow.
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    @task()
    def load_restaurants():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver("stg.ordersystem_restaurants")

        # Инициализируем подключение к MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем MongoReader для получения данных.
        collection_reader = MongoReader(mongo_connect, "restaurants")

        # Инициализируем класс, реализующий бизнес-логику загрузки данных.
        loader = RestaurantLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_users():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver("stg.ordersystem_users")

        # Инициализируем подключение к MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем MongoReader для получения данных.
        collection_reader = MongoReader(mongo_connect, "users")

        # Инициализируем класс, реализующий бизнес-логику загрузки данных.
        loader = UserLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    @task()
    def load_orders():
        # Инициализируем класс, в котором реализована логика сохранения.
        pg_saver = PgSaver("stg.ordersystem_orders")

        # Инициализируем подключение к MongoDB.
        mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)

        # Инициализируем MongoReader для получения данных.
        collection_reader = MongoReader(mongo_connect, "orders")

        # Инициализируем класс, реализующий бизнес-логику загрузки данных.
        loader = OrderLoader(collection_reader, dwh_pg_connect, pg_saver, log)

        # Запускаем копирование данных.
        loader.run_copy()

    # Определяем последовательность выполнения задач.
    restaurant_loader = load_restaurants()
    load_users = load_users()
    order_users = load_orders()
    restaurant_loader >> load_users >> order_users


order_stg_dag = stg_order_system()  # noqa

