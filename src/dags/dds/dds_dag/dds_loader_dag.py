import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from examples.dds.utils import max_ts


log = logging.getLogger(__name__)


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения
    catchup=False,  # Не запускать пропущенные интервалы
    tags=['sprint5', 'dds', 'origin', 'example'],  # Теги
    is_paused_upon_creation=True  # Даг создается в остановленном состоянии
)


def dds_dag():
    @task(task_id="user_load")
    def user_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
            INSERT INTO dds.dm_users (id, user_id, user_login, user_name)
            SELECT 
                id,
                (object_value::json ->> '_id')::text AS user_id,
                (object_value::json ->> 'login')::text AS user_login,
                (object_value::json ->> 'name')::text AS user_name
            FROM stg.ordersystem_users
            ON CONFLICT (id) DO UPDATE 
            SET 
                user_id = EXCLUDED.user_id,
                user_login = EXCLUDED.user_login,
                user_name = EXCLUDED.user_name;
        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('dm_users', 'stg.ordersystem_users', 'update_ts')

    @task(task_id="restaurant_load")
    def restaurant_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
                        INSERT INTO dds.dm_restaurants (id, restaurant_id, restaurant_name, active_from, active_to)
                            SELECT 
                                id,
                                (object_value::json ->> '_id')::text AS restaurant_id,
                                (object_value::json ->> 'name')::text AS restaurant_name,
                                update_ts AS active_from,
                                '2099-12-31 00:00:00.000'::timestamp AS active_to  -- Это значение по умолчанию для active_to
                            FROM stg.ordersystem_restaurants
                            ON CONFLICT (id) DO UPDATE
                            SET 
                                restaurant_id = EXCLUDED.restaurant_id,
                                restaurant_name = EXCLUDED.restaurant_name,
                                active_from = EXCLUDED.active_from,
                                active_to = '2099-12-31 00:00:00.000'::timestamp;  -- обновляем active_to на значение по умолчанию
                    """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('dm_restaurants', 'stg.ordersystem_restaurants', 'update_ts')


    @task(task_id="timestamp_load")
    def timestamp_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
                        insert into dds.dm_timestamps
                        select id,
                            (object_value::json ->> 'date')::timestamp as ts, 
                            EXTRACT(YEAR FROM (object_value::json ->> 'date')::timestamp)::int as year, 
                            EXTRACT(month FROM (object_value::json ->> 'date')::timestamp)::int as month, 
                            EXTRACT(day FROM (object_value::json ->> 'date')::timestamp)::int as day,
                            ((object_value::json ->> 'date')::timestamp)::time as time,
                            ((object_value::json ->> 'date')::timestamp)::date as date
                        from stg.ordersystem_orders
                            ON CONFLICT (id) DO UPDATE
                                SET 
                                    ts = EXCLUDED.ts,
                                    year = EXCLUDED.year,
                                    month = EXCLUDED.month,
                                    day = EXCLUDED.day,
                                    time = EXCLUDED.time,
                                    date = EXCLUDED.date
                        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('dm_timestamps', 'stg.ordersystem_orders','update_ts')

    @task(task_id="product_load")
    def product_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
             insert into dds.dm_products(
                            restaurant_id
                            ,product_id
                            ,product_name
                            ,product_price
                            ,active_from
                            ,active_to)
                select 
                        id as restaurant_id, 
                        jsonb_array_elements((object_value::json -> 'menu')::jsonb)::jsonb->>'_id' as product_id , 
                        jsonb_array_elements((object_value::json -> 'menu')::jsonb)::jsonb->>'name' as product_name, 
                        (jsonb_array_elements((object_value::json -> 'menu')::jsonb)::jsonb->>'price')::numeric(14, 2) as product_price, 
                        update_ts::timestamp as active_from,
                        '2099-12-31 00:00:00.000'::timestamp as active_to
                from stg.ordersystem_restaurants
            ON CONFLICT (restaurant_id,product_id) DO UPDATE
                SET 
                    active_from = EXCLUDED.active_from,
                    active_to = EXCLUDED.active_to,
                    product_name = EXCLUDED.product_name,
                    product_price = EXCLUDED.product_price
                        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('dm_product', 'stg.ordersystem_restaurants', 'update_ts')

    @task(task_id="orders_load")
    def orders_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
 insert into dds.dm_orders(
                            user_id
                            ,restaurant_id
                            ,timestamp_id
                            ,order_key                                                                                                                                                                                                                                  
                            ,order_status
                            ,delivery_id)
                                                         with main as (
            select 
                du.id as user_id
                ,dr.id		as restaurant_id
                ,(jsonb_array_elements((object_value::json -> 'statuses')::jsonb)::jsonb->>'dttm')::timestamp as dttm_status  
                ,jsonb_array_elements((object_value::json -> 'statuses')::jsonb)::jsonb->>'status' as order_status 
                ,s.object_value::json->>'_id'		as order_key
                ,s.update_ts
                ,dd.id as delivery_id
            from stg.ordersystem_orders s
            left join dds.dm_users du on du.user_id = (s.object_value::json->'user'->>'id')
            left join dds.dm_restaurants dr on dr.restaurant_id = (s.object_value::json->'restaurant'->>'id')
            left join stg.deliveries d on (s.object_id) = (d."data"::json ->> 'order_id') 
            left join dds.dm_deliveries dd	on (d."data"::json->>'delivery_id') = dd.delivery_key )
                select 
                    user_id
                    ,restaurant_id
                    ,dt.id as timestamp_id
                    ,main.order_key
                    ,order_status
                    ,delivery_id
                from main
                right join(select order_key,  max(dttm_status) as dttm_status from main group by order_key ) as lasts on lasts.order_key=main.order_key and lasts.dttm_status = main.dttm_status
                left join dds.dm_timestamps dt on dt.ts::timestamp(0) = main.dttm_status::timestamp(0)
                ON CONFLICT (order_key) DO UPDATE
                                            SET 
                                                user_id = EXCLUDED.user_id,
                                                restaurant_id = EXCLUDED.restaurant_id,
                                                timestamp_id = EXCLUDED.timestamp_id,
                                                order_status = EXCLUDED.order_status,
                                                delivery_id = EXCLUDED.delivery_id
                                                                                    """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('dm_orders', 'stg.ordersystem_orders', 'update_ts')


    @task(task_id="fct_product_sales")
    def product_sales_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
             insert into dds.fct_product_sales(
                product_id
                ,order_id
                ,count
                ,price
                ,total_sum
                ,bonus_payment
                ,bonus_grant)
        with main as (
            SELECT 
                 event_value::json->>'user_id' as  user_id
                ,event_value::json->>'order_id' as  order_id
                ,jsonb_array_elements((event_value::json->>'product_payments')::jsonb)->>'product_id' as product_id
                ,(jsonb_array_elements((event_value::json->>'product_payments')::jsonb)->>'price')::numeric as price
                ,(jsonb_array_elements((event_value::json->>'product_payments')::jsonb)->>'quantity')::int as quantity
                ,(jsonb_array_elements((event_value::json->>'product_payments')::jsonb)->>'bonus_payment')::numeric as bonus_payment
                ,(jsonb_array_elements((event_value::json->>'product_payments')::jsonb)->>'bonus_grant')::numeric as bonus_grant
            FROM stg.bonussystem_events AS be
            where be.event_type ='bonus_transaction')
                select 
                    dp.id as product_id
                    ,dmo.id as order_id
                    --,order_id
                    ,quantity as count
                    ,price as price
                    ,price*quantity as total_sum
                    ,bonus_payment
                    ,bonus_grant
                from main
                left join dds.dm_orders dmo on dmo.order_key = main.order_id
                left join dds.dm_products dp on dp.product_id = main.product_id
                    where dmo.id is not null
                ON CONFLICT (product_id, order_id) DO UPDATE
                SET 
                    count = EXCLUDED.count,
                    price = EXCLUDED.price,
                    total_sum = EXCLUDED.total_sum,
                    bonus_payment = EXCLUDED.bonus_payment,
                    bonus_grant = EXCLUDED.bonus_grant
                                                                                    """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('fct_product_sales', 'stg.bonussystem_events', 'event_ts')

    @task(task_id="dm_courier")
    def courier_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
        insert into  dds.dm_courier(id_courier, name)
        select  
            "data" ->> '_id' as id_courier
            ,"data" ->> 'name' as name
        from stg.couriers c
            on conflict (id_courier) do update 
                set name=EXCLUDED.name
                        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('dm_courier', 'stg.couriers', 'update_ts')


    @task(task_id="dm_delivery_address")
    def delivery_address_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
        insert into dds.dm_delivery_address(address)
            select 
                distinct ("data" ->> 'address')::varchar  as address
            from stg.deliveries d
                on conflict (address) do nothing
                        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('dm_delivery_address', 'stg.deliveries', 'update_ts')



    @task(task_id="dm_delivery_ts")
    def delivery_ts_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
        insert into dds.dm_delivery_ts(delivery_ts)
            select 
                distinct ("data" ->> 'delivery_ts')::timestamp  as delivery_ts
            from stg.deliveries d
            order by ("data" ->> 'delivery_ts')::timestamp
        on conflict (delivery_ts) do nothing;
                        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('dm_delivery_ts', 'stg.deliveries', 'update_ts')


    @task(task_id="dm_deliveries")
    def deliveries_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
        insert into  dds.dm_deliveries (
                                        address_id
                                        ,courier_id
                                        ,delivery_key
                                        ,delivery_ts_id)
            select  
                dda.id  as address_id
                , dc.id as courier_id
                ,("data" ->> 'delivery_id') as delivery_key
                ,ts.id  as delivery_ts_id
            from stg.deliveries d 
            left join dds.dm_courier dc on 
                    dc.id_courier = ("data" ->> 'courier_id')
            left join dds.dm_delivery_ts ts on 
                    ts.delivery_ts = ("data" ->> 'delivery_ts')::timestamp
            left join dds.dm_delivery_address dda on 
                    dda.address = ("data" ->> 'address')
        on conflict (delivery_key) do update set 
                        courier_id=EXCLUDED.courier_id,			
                        delivery_ts_id=EXCLUDED.delivery_ts_id,
                        address_id=EXCLUDED.address_id
                        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('dm_deliveries', 'stg.deliveries', 'update_ts')

    @task(task_id="fct_delivery")
    def fct_delivery_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
        insert into dds.fct_delivery(delivery_id,
                                    "sum",
                                    rate,
                                    tip_sum)                                                
            select 
                dd.id as delivery_id
                ,("data" ->> 'sum')::numeric as "sum"
                ,("data" ->> 'rate')::numeric as rate
                ,("data" ->> 'tip_sum')::numeric as tip_sum
            from stg.deliveries d 
            left join dds.dm_deliveries dd on 
                    dd.delivery_key = ("data" ->> 'delivery_id')
                where dd.id is not null
                    order by delivery_id
        on conflict (delivery_id) do update
            set delivery_id = EXCLUDED.delivery_id,
                "sum" = EXCLUDED.sum,
                rate = EXCLUDED.rate,
                tip_sum = EXCLUDED.tip_sum
                        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")
        max_ts('fct_delivery', 'stg.deliveries', 'update_ts')

    # Запуск таска
    users_load=user_loader()
    restaurants_load=restaurant_loader()
    timestamps_load=timestamp_loader()
    product_load=product_loader()
    order_load=orders_loader()
    product_sales_load=product_sales_loader()
    dm_courier_load=courier_loader()
    dm_delivery_address_load=delivery_address_loader()
    dm_delivery_ts_load=delivery_ts_loader()
    dm_deliveries_load=deliveries_loader()
    fct_delivery_load=fct_delivery_loader() 


    [dm_courier_load, dm_delivery_ts_load, dm_delivery_address_load, users_load, restaurants_load, timestamps_load, product_load]  >> dm_deliveries_load >> order_load >> [product_sales_load, fct_delivery_load]

dds_dag=dds_dag()
