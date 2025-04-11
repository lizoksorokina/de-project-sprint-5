import logging
import pendulum
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook


log = logging.getLogger(__name__)


@dag(
    schedule_interval=None,  
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения
    catchup=False,  # Не запускать пропущенные интервалы
    tags=['sprint5', 'cdm', 'origin', 'example'],  # Теги
    is_paused_upon_creation=True  # Даг создается в остановленном состоянии
)


def cdm_dag():
    @task(task_id="dm_settlement_report")
    def dm_settlement_report_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
            insert into cdm.dm_settlement_report(
                                        restaurant_id
                                        ,restaurant_name
                                        ,settlement_date
                                        ,orders_count
                                        ,orders_total_sum
                                        ,orders_bonus_payment_sum
                                        ,orders_bonus_granted_sum
                                        ,order_processing_fee
                                        ,restaurant_reward_sum)
        select 
            dr.id as restaurant_id
            ,dr.restaurant_name
            ,date_trunc('day', ts)::date as settlement_date
            ,count(distinct do2.id) as orders_count
            ,sum(total_sum) as orders_total_sum
            ,sum(bonus_payment) as orders_bonus_payment_sum
            ,sum(bonus_grant) as orders_bonus_granted_sum
            ,sum(total_sum)*0.25	as order_processing_fee
            ,sum(total_sum) -sum(bonus_payment) - sum(total_sum)*0.25 as restaurant_reward_sum
        from dds.fct_product_sales fps
        left join dds.dm_products dp on dp.id = fps.product_id
        left join dds.dm_restaurants dr on dp.restaurant_id = dr.id
        left join dds.dm_orders do2 on fps.order_id = do2.id
        left join dds.dm_timestamps dt on dt.id = do2.timestamp_id
            where do2.order_status='CLOSED'
                group by 	
                    dr.id 
                    ,dr.restaurant_name
                    ,date_trunc('day', ts)::date
        ON CONFLICT (settlement_date, restaurant_id) DO UPDATE
            SET 
                restaurant_name = EXCLUDED.restaurant_name,
                orders_count = EXCLUDED.orders_count,
                orders_total_sum = EXCLUDED.orders_total_sum,
                orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,
                orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,
                order_processing_fee = EXCLUDED.order_processing_fee,
                restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;
        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")


    @task(task_id="courier_payouts")
    def courier_payouts_loader():
        hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")
        sql_query = """
                insert into cdm.courier_payouts(courier_id
                            ,courier_name
                            ,settlement_year
                            ,settlement_month
                            ,orders_count
                            ,orders_total_sum
                            ,rate_avg
                            ,order_processing_fee
                            ,courier_order_sum
                            ,courier_tips_sum
                            ,courier_reward_sum)
                select 
                    dd.courier_id
                    , name as courier_name
                    , date_part('year', ts.delivery_ts)::int as settlement_year
                    , date_part('month', ts.delivery_ts)::int as settlement_month
                    , count(*) as orders_count
                    ,sum(sum) as orders_total_sum
                    ,avg(rate) as rate_avg
                    ,sum(sum) * 0.25 as order_processing_fee
                    ,sum(
                        case 
                            when rate<4 and 0.05*sum>100 then 0.05*sum 
                            when rate<4 and 0.05*sum<=100 then  100
                            when 4<=rate and rate <4.5 and 0.07*sum>150 then 0.07*sum 
                            when 4<=rate and rate <4.5 and 0.07*sum<=150 then 150
                            when 4.5<=rate and rate <4.9 and 0.08*sum>175 then 0.08*sum 
                            when 4.5<=rate and rate <4.9 and 0.08*sum<=175 then 175
                            when 4.9<=rate and 0.1*sum>200 then 0.1*sum 
                            when 4.9<=rate and 0.1*sum<=200 then  200
                        end 
                        )as courier_order_sum
                    ,sum(tip_sum) as courier_tips_sum
                    ,sum(
                            case 
                                when rate<4 and 0.05*sum>100 then 0.05*sum 
                                when rate<4 and 0.05*sum<=100 then  100
                                when 4<=rate and rate <4.5 and 0.07*sum>150 then 0.07*sum 
                                when 4<=rate and rate <4.5 and 0.07*sum<=150 then 150
                                when 4.5<=rate and rate <4.9 and 0.08*sum>175 then 0.08*sum 
                                when 4.5<=rate and rate <4.9 and 0.08*sum<=175 then 175
                                when 4.9<=rate and 0.1*sum>200 then 0.1*sum 
                                when 4.9<=rate and 0.1*sum<=200 then  200
                            end 
                            )+ sum(tip_sum) * 0.95 as courier_order_sumfrom 
                from dds.fct_delivery fd
                left join dds.dm_deliveries dd on fd.delivery_id = dd.id
                left join dds.dm_courier dc on dc.id = dd.courier_id
                left join dds.dm_delivery_ts ts on ts.id = dd.delivery_ts_id
                group by
                    dd.courier_id
                    , name 
                    , date_part('year', ts.delivery_ts)::int 
                    , date_part('month', ts.delivery_ts)::int
                    on conflict (courier_id, settlement_year, settlement_month) do update
                set 
                    courier_name = EXCLUDED.courier_name
                    ,orders_count = EXCLUDED.orders_count
                    ,orders_total_sum = EXCLUDED.orders_total_sum
                    ,rate_avg = EXCLUDED.rate_avg
                    ,order_processing_fee = EXCLUDED.order_processing_fee
                    ,courier_order_sum = EXCLUDED.courier_order_sum
                    ,courier_tips_sum = EXCLUDED.courier_tips_sum
                    ,courier_reward_sum = EXCLUDED.courier_reward_sum;
        """

        log.info("Executing SQL query...")
        hook.run(sql_query)
        log.info("SQL query executed successfully.")


    # Запуск таска
    courier_payouts=dm_settlement_report_loader()
    dm_settlement_report=courier_payouts_loader()

    [courier_payouts,dm_settlement_report]

cdm_load=cdm_dag()
