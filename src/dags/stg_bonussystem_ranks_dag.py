import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values

log = logging.getLogger(__name__)

ORIGIN_CONN_ID = "PG_ORIGIN_BONUS_SYSTEM_CONNECTION"
DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"
WF_KEY = "bonussystem_events_origin_to_stg_workflow"
LAST_LOADED_ID_KEY = "last_loaded_id"

with DAG(
    dag_id="stg_bonussystem_ranks_dag",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["stg", "bonussystem", "origin"],
) as dag:

    @task(task_id="ranks_load")
    def load_ranks():
        origin_hook = PostgresHook(ORIGIN_CONN_ID)
        dwh_hook = PostgresHook(DWH_CONN_ID)
        rows = origin_hook.get_records(
            sql="SELECT id, name, bonus_percent, min_payment_threshold FROM ranks;"
        )
        dwh_hook.run(sql="TRUNCATE TABLE stg.bonussystem_ranks;")
        dwh_hook.insert_rows(
            table="stg.bonussystem_ranks",
            rows=rows,
            target_fields=["id", "name", "bonus_percent", "min_payment_threshold"],
        )
        log.info("Loaded %s rows into stg.bonussystem_ranks", len(rows))

    @task(task_id="users_load")
    def load_users():
        origin_hook = PostgresHook(ORIGIN_CONN_ID)
        dwh_hook = PostgresHook(DWH_CONN_ID)
        rows = origin_hook.get_records(sql="SELECT order_user_id FROM users;")
        dwh_hook.run(sql="TRUNCATE TABLE stg.bonussystem_users;")
        dwh_hook.insert_rows(
            table="stg.bonussystem_users",
            rows=rows,
            target_fields=["order_user_id"],
        )
        log.info("Loaded %s rows into stg.bonussystem_users", len(rows))

    @task(task_id="events_load")
    def load_events():
        origin_hook = PostgresHook(ORIGIN_CONN_ID)
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()

        try:
            with dwh_conn:  
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM stg.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY,),
                    )
                    wf_row = cur.fetchone()
                    if wf_row:
                        wf_id, wf_settings = wf_row
                        last_loaded_id = wf_settings.get(LAST_LOADED_ID_KEY, 0)
                    else:
                        wf_id, last_loaded_id = None, 0

                    events = origin_hook.get_records(
                        sql="""
                            SELECT id, event_ts, event_type, event_value
                            FROM outbox
                            WHERE id > %s
                            ORDER BY id ASC;
                        """,
                        parameters=(last_loaded_id,),
                    )

                    if not events:
                        log.info("No new events found after id=%s", last_loaded_id)
                        return

                    execute_values(
                        cur,
                        "INSERT INTO stg.bonussystem_events "
                        "(id, event_ts, event_type, event_value) VALUES %s;",
                        events,
                    )

                    new_last_loaded_id = events[-1][0]
                    settings_payload = Json({LAST_LOADED_ID_KEY: new_last_loaded_id})

                    if wf_id is not None:
                        cur.execute(
                            "UPDATE stg.srv_wf_settings "
                            "SET workflow_settings = %s WHERE id = %s;",
                            (settings_payload, wf_id),
                        )
                    else:
                        cur.execute(
                            "INSERT INTO stg.srv_wf_settings (workflow_key, workflow_settings) "
                            "VALUES (%s, %s);",
                            (WF_KEY, settings_payload),
                        )

                    log.info(
                        "Loaded %s events into stg.bonussystem_events, cursor moved to id=%s",
                        len(events),
                        new_last_loaded_id,
                    )
        finally:
            dwh_conn.close()

    load_ranks()
    load_users()
    load_events()