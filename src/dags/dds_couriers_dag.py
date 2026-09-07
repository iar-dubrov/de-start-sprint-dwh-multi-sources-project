import json
import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

log = logging.getLogger(__name__)

DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"
LAST_LOADED_TS_KEY = "last_loaded_ts"
WF_KEY_DM_COURIERS = "dm_couriers_stg_to_dds_workflow"
WF_KEY_DM_ORDERS_COURIER_LINK = "dm_orders_courier_link_stg_to_dds_workflow"

with DAG(
    dag_id="dds_couriers_dag",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["dds", "stg-to-dds", "delivery-system"],
) as dag:

    @task(task_id="dm_couriers_load")
    def load_dm_couriers():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM dds.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY_DM_COURIERS,),
                    )
                    wf_row = cur.fetchone()
                    wf_id, last_loaded_ts = None, None
                    if wf_row:
                        wf_id, wf_settings = wf_row
                        raw_ts = wf_settings.get(LAST_LOADED_TS_KEY)
                        if raw_ts:
                            last_loaded_ts = datetime.fromisoformat(raw_ts)

                    if last_loaded_ts:
                        cur.execute(
                            "SELECT object_value, load_dttm "
                            "FROM stg.deliverysystem_couriers "
                            "WHERE load_dttm > %s "
                            "ORDER BY load_dttm ASC;",
                            (last_loaded_ts,),
                        )
                    else:
                        cur.execute(
                            "SELECT object_value, load_dttm "
                            "FROM stg.deliverysystem_couriers "
                            "ORDER BY load_dttm ASC;"
                        )
                    stg_rows = cur.fetchall()

                    if not stg_rows:
                        log.info("No new couriers found in stg after %s", last_loaded_ts)
                        return

                    for object_value, _load_dttm in stg_rows:
                        doc = (
                            object_value
                            if isinstance(object_value, dict)
                            else json.loads(object_value)
                        )
                        courier_id = str(doc["_id"])
                        courier_name = doc["name"]

                        cur.execute(
                            "SELECT id FROM dds.dm_couriers WHERE courier_id = %s;",
                            (courier_id,),
                        )
                        if cur.fetchone():
                            cur.execute(
                                "UPDATE dds.dm_couriers "
                                "SET courier_name = %s "
                                "WHERE courier_id = %s;",
                                (courier_name, courier_id),
                            )
                        else:
                            cur.execute(
                                "INSERT INTO dds.dm_couriers (courier_id, courier_name) "
                                "VALUES (%s, %s);",
                                (courier_id, courier_name),
                            )

                    new_last_loaded_ts = stg_rows[-1][1]
                    settings_payload = Json({LAST_LOADED_TS_KEY: new_last_loaded_ts.isoformat()})

                    if wf_id is not None:
                        cur.execute(
                            "UPDATE dds.srv_wf_settings "
                            "SET workflow_settings = %s WHERE id = %s;",
                            (settings_payload, wf_id),
                        )
                    else:
                        cur.execute(
                            "INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings) "
                            "VALUES (%s, %s);",
                            (WF_KEY_DM_COURIERS, settings_payload),
                        )

                    log.info(
                        "Processed %s stg rows into dds.dm_couriers, cursor moved to %s",
                        len(stg_rows),
                        new_last_loaded_ts,
                    )
        finally:
            dwh_conn.close()

    @task(task_id="dm_orders_courier_link_load")
    def load_dm_orders_courier_link():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM dds.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY_DM_ORDERS_COURIER_LINK,),
                    )
                    wf_row = cur.fetchone()
                    wf_id, last_loaded_ts = None, None
                    if wf_row:
                        wf_id, wf_settings = wf_row
                        raw_ts = wf_settings.get(LAST_LOADED_TS_KEY)
                        if raw_ts:
                            last_loaded_ts = datetime.fromisoformat(raw_ts)

                    if last_loaded_ts:
                        cur.execute(
                            "SELECT object_value, load_dttm "
                            "FROM stg.deliverysystem_deliveries "
                            "WHERE load_dttm > %s "
                            "ORDER BY load_dttm ASC;",
                            (last_loaded_ts,),
                        )
                    else:
                        cur.execute(
                            "SELECT object_value, load_dttm "
                            "FROM stg.deliverysystem_deliveries "
                            "ORDER BY load_dttm ASC;"
                        )
                    stg_rows = cur.fetchall()

                    if not stg_rows:
                        log.info("No new deliveries found in stg after %s", last_loaded_ts)
                        return

                    matched_count = 0
                    skipped_no_order = 0
                    skipped_no_courier = 0

                    for object_value, _load_dttm in stg_rows:
                        doc = (
                            object_value
                            if isinstance(object_value, dict)
                            else json.loads(object_value)
                        )
                        order_key = str(doc["order_id"])
                        courier_natural_id = str(doc["courier_id"])

                        cur.execute(
                            "SELECT id FROM dds.dm_orders WHERE order_key = %s;",
                            (order_key,),
                        )
                        order_row = cur.fetchone()
                        if order_row is None:
                            skipped_no_order += 1
                            continue

                        cur.execute(
                            "SELECT id FROM dds.dm_couriers WHERE courier_id = %s;",
                            (courier_natural_id,),
                        )
                        courier_row = cur.fetchone()
                        if courier_row is None:
                            skipped_no_courier += 1
                            continue

                        cur.execute(
                            "UPDATE dds.dm_orders SET courier_id = %s WHERE id = %s;",
                            (courier_row[0], order_row[0]),
                        )
                        matched_count += 1

                    new_last_loaded_ts = stg_rows[-1][1]
                    settings_payload = Json({LAST_LOADED_TS_KEY: new_last_loaded_ts.isoformat()})

                    if wf_id is not None:
                        cur.execute(
                            "UPDATE dds.srv_wf_settings "
                            "SET workflow_settings = %s WHERE id = %s;",
                            (settings_payload, wf_id),
                        )
                    else:
                        cur.execute(
                            "INSERT INTO dds.srv_wf_settings (workflow_key, workflow_settings) "
                            "VALUES (%s, %s);",
                            (WF_KEY_DM_ORDERS_COURIER_LINK, settings_payload),
                        )

                    log.info(
                        "Processed %s stg rows for dm_orders.courier_id (matched=%s, "
                        "skipped: no_order=%s no_courier=%s), cursor moved to %s",
                        len(stg_rows),
                        matched_count,
                        skipped_no_order,
                        skipped_no_courier,
                        new_last_loaded_ts,
                    )
        finally:
            dwh_conn.close()

    load_dm_couriers() >> load_dm_orders_courier_link()