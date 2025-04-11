import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

def max_ts(workflow_key: str, table_name: str, column_name:str):
    hook = PostgresHook(postgres_conn_id="PG_WAREHOUSE_CONNECTION")

    sql_query = f"""
        WITH max_ts AS (
            SELECT 
                json_build_object(
                    'update_ts', max({column_name})::timestamp(0)::text,
                    'current_timestamp', max(current_timestamp)::timestamp(0)::text
                ) AS max_ts
            FROM {table_name}
        )
        INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings)
        SELECT 
            %s,  
            max_ts  
        FROM max_ts
        ON CONFLICT (workflow_key) DO UPDATE 
        SET workflow_settings = EXCLUDED.workflow_settings;
    """

    log.info("Executing SQL query...")
    hook.run(sql_query, parameters=(workflow_key,))
    log.info("SQL query executed successfully.")
