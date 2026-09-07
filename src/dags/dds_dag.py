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
WF_KEY_DM_USERS = "dm_users_stg_to_dds_workflow"
WF_KEY_DM_RESTAURANTS = "dm_restaurants_stg_to_dds_workflow"
WF_KEY_DM_TIMESTAMPS = "dm_timestamps_stg_to_dds_workflow"
WF_KEY_DM_PRODUCTS = "dm_products_stg_to_dds_workflow"
WF_KEY_DM_ORDERS = "dm_orders_stg_to_dds_workflow"
WF_KEY_FCT_PRODUCT_SALES = "fct_product_sales_stg_to_dds_workflow"
WF_KEY_FCT_DELIVERIES = "fct_deliveries_stg_to_dds_workflow"
ACTIVE_TO_INFINITY = datetime(2099, 12, 31)
FINAL_ORDER_STATUSES = ("CLOSED", "CANCELLED")

with DAG(
    dag_id="dds_dag",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["dds", "stg-to-dds"],
) as dag:

    @task(task_id="dm_users_load")
    def load_dm_users():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM dds.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY_DM_USERS,),
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
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_users "
                            "WHERE update_ts > %s "
                            "ORDER BY update_ts ASC;",
                            (last_loaded_ts,),
                        )
                    else:
                        cur.execute(
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_users "
                            "ORDER BY update_ts ASC;"
                        )
                    stg_rows = cur.fetchall()

                    if not stg_rows:
                        log.info("No new users found in stg after %s", last_loaded_ts)
                        return

                    for object_value, _ts in stg_rows:
                        doc = (
                            object_value
                            if isinstance(object_value, dict)
                            else json.loads(object_value)
                        )
                        user_id = str(doc["_id"])
                        user_name = doc["name"]
                        user_login = doc["login"]

                        cur.execute(
                            "SELECT id FROM dds.dm_users WHERE user_id = %s;",
                            (user_id,),
                        )
                        if cur.fetchone():
                            cur.execute(
                                "UPDATE dds.dm_users "
                                "SET user_name = %s, user_login = %s "
                                "WHERE user_id = %s;",
                                (user_name, user_login, user_id),
                            )
                        else:
                            cur.execute(
                                "INSERT INTO dds.dm_users (user_id, user_name, user_login) "
                                "VALUES (%s, %s, %s);",
                                (user_id, user_name, user_login),
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
                            (WF_KEY_DM_USERS, settings_payload),
                        )

                    log.info(
                        "Processed %s stg rows into dds.dm_users, cursor moved to %s",
                        len(stg_rows),
                        new_last_loaded_ts,
                    )
        finally:
            dwh_conn.close()

    @task(task_id="dm_restaurants_load")
    def load_dm_restaurants():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM dds.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY_DM_RESTAURANTS,),
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
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_restaurants "
                            "WHERE update_ts > %s "
                            "ORDER BY update_ts ASC;",
                            (last_loaded_ts,),
                        )
                    else:
                        cur.execute(
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_restaurants "
                            "ORDER BY update_ts ASC;"
                        )
                    stg_rows = cur.fetchall()

                    if not stg_rows:
                        log.info("No new restaurants found in stg after %s", last_loaded_ts)
                        return

                    for object_value, row_update_ts in stg_rows:
                        doc = (
                            object_value
                            if isinstance(object_value, dict)
                            else json.loads(object_value)
                        )
                        restaurant_id = str(doc["_id"])
                        restaurant_name = doc["name"]
                        active_from = row_update_ts

                        cur.execute(
                            "SELECT id, restaurant_name "
                            "FROM dds.dm_restaurants "
                            "WHERE restaurant_id = %s AND active_to = %s;",
                            (restaurant_id, ACTIVE_TO_INFINITY),
                        )
                        current = cur.fetchone()

                        if current is None:
                            cur.execute(
                                "INSERT INTO dds.dm_restaurants "
                                "(restaurant_id, restaurant_name, active_from, active_to) "
                                "VALUES (%s, %s, %s, %s);",
                                (restaurant_id, restaurant_name, active_from, ACTIVE_TO_INFINITY),
                            )
                        elif current[1] != restaurant_name:
                            cur.execute(
                                "UPDATE dds.dm_restaurants SET active_to = %s WHERE id = %s;",
                                (active_from, current[0]),
                            )
                            cur.execute(
                                "INSERT INTO dds.dm_restaurants "
                                "(restaurant_id, restaurant_name, active_from, active_to) "
                                "VALUES (%s, %s, %s, %s);",
                                (restaurant_id, restaurant_name, active_from, ACTIVE_TO_INFINITY),
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
                            (WF_KEY_DM_RESTAURANTS, settings_payload),
                        )

                    log.info(
                        "Processed %s stg rows into dds.dm_restaurants, cursor moved to %s",
                        len(stg_rows),
                        new_last_loaded_ts,
                    )
        finally:
            dwh_conn.close()

    @task(task_id="dm_timestamps_load")
    def load_dm_timestamps():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM dds.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY_DM_TIMESTAMPS,),
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
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_orders "
                            "WHERE update_ts > %s "
                            "ORDER BY update_ts ASC;",
                            (last_loaded_ts,),
                        )
                    else:
                        cur.execute(
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_orders "
                            "ORDER BY update_ts ASC;"
                        )
                    stg_rows = cur.fetchall()

                    if not stg_rows:
                        log.info("No new orders found in stg after %s", last_loaded_ts)
                        return

                    for object_value, _row_update_ts in stg_rows:
                        doc = (
                            object_value
                            if isinstance(object_value, dict)
                            else json.loads(object_value)
                        )

                        final_status = str(doc.get("final_status", "")).upper()
                        if final_status not in FINAL_ORDER_STATUSES:
                            continue

                        raw_date = doc["date"]
                        ts = (
                            raw_date
                            if isinstance(raw_date, datetime)
                            else datetime.fromisoformat(raw_date)
                        )
                        ts = ts.replace(microsecond=0)

                        cur.execute(
                            "SELECT id FROM dds.dm_timestamps WHERE ts = %s;",
                            (ts,),
                        )
                        if cur.fetchone():
                            continue

                        cur.execute(
                            "INSERT INTO dds.dm_timestamps (ts, year, month, day, date, time) "
                            "VALUES (%s, %s, %s, %s, %s, %s);",
                            (ts, ts.year, ts.month, ts.day, ts.date(), ts.time()),
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
                            (WF_KEY_DM_TIMESTAMPS, settings_payload),
                        )

                    log.info(
                        "Processed %s stg rows into dds.dm_timestamps, cursor moved to %s",
                        len(stg_rows),
                        new_last_loaded_ts,
                    )
        finally:
            dwh_conn.close()

    @task(task_id="dm_products_load")
    def load_dm_products():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM dds.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY_DM_PRODUCTS,),
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
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_orders "
                            "WHERE update_ts > %s "
                            "ORDER BY update_ts ASC;",
                            (last_loaded_ts,),
                        )
                    else:
                        cur.execute(
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_orders "
                            "ORDER BY update_ts ASC;"
                        )
                    stg_rows = cur.fetchall()

                    if not stg_rows:
                        log.info("No new orders found in stg after %s", last_loaded_ts)
                        return

                    for object_value, row_update_ts in stg_rows:
                        doc = (
                            object_value
                            if isinstance(object_value, dict)
                            else json.loads(object_value)
                        )
                        active_from = row_update_ts
                        restaurant_natural_id = str(doc["restaurant"]["id"])

                        cur.execute(
                            "SELECT id FROM dds.dm_restaurants "
                            "WHERE restaurant_id = %s AND active_to = %s;",
                            (restaurant_natural_id, ACTIVE_TO_INFINITY),
                        )
                        restaurant_row = cur.fetchone()
                        if restaurant_row is None:
                            continue
                        restaurant_sk = restaurant_row[0]

                        for item in doc.get("order_items", []):
                            product_id = str(item["id"])
                            product_name = item["name"]
                            product_price = item["price"]

                            cur.execute(
                                "SELECT id, product_name, product_price "
                                "FROM dds.dm_products "
                                "WHERE product_id = %s AND restaurant_id = %s AND active_to = %s;",
                                (product_id, restaurant_sk, ACTIVE_TO_INFINITY),
                            )
                            current = cur.fetchone()

                            if current is None:
                                cur.execute(
                                    "INSERT INTO dds.dm_products "
                                    "(product_id, product_name, product_price, active_from, active_to, restaurant_id) "
                                    "VALUES (%s, %s, %s, %s, %s, %s);",
                                    (
                                        product_id,
                                        product_name,
                                        product_price,
                                        active_from,
                                        ACTIVE_TO_INFINITY,
                                        restaurant_sk,
                                    ),
                                )
                            elif current[1] != product_name or float(current[2]) != float(product_price):
                                cur.execute(
                                    "UPDATE dds.dm_products SET active_to = %s WHERE id = %s;",
                                    (active_from, current[0]),
                                )
                                cur.execute(
                                    "INSERT INTO dds.dm_products "
                                    "(product_id, product_name, product_price, active_from, active_to, restaurant_id) "
                                    "VALUES (%s, %s, %s, %s, %s, %s);",
                                    (
                                        product_id,
                                        product_name,
                                        product_price,
                                        active_from,
                                        ACTIVE_TO_INFINITY,
                                        restaurant_sk,
                                    ),
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
                            (WF_KEY_DM_PRODUCTS, settings_payload),
                        )

                    log.info(
                        "Processed %s stg rows into dds.dm_products, cursor moved to %s",
                        len(stg_rows),
                        new_last_loaded_ts,
                    )
        finally:
            dwh_conn.close()

    @task(task_id="dm_orders_load")
    def load_dm_orders():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM dds.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY_DM_ORDERS,),
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
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_orders "
                            "WHERE update_ts > %s "
                            "ORDER BY update_ts ASC;",
                            (last_loaded_ts,),
                        )
                    else:
                        cur.execute(
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_orders "
                            "ORDER BY update_ts ASC;"
                        )
                    stg_rows = cur.fetchall()

                    if not stg_rows:
                        log.info("No new orders found in stg after %s", last_loaded_ts)
                        return

                    matched_count = 0
                    skipped_no_restaurant = 0
                    skipped_no_timestamp = 0
                    skipped_no_user = 0

                    for object_value, _row_update_ts in stg_rows:
                        doc = (
                            object_value
                            if isinstance(object_value, dict)
                            else json.loads(object_value)
                        )

                        order_key = str(doc["_id"])
                        order_status = str(doc.get("final_status", "")).upper()

                        raw_date = doc["date"]
                        order_ts = (
                            raw_date
                            if isinstance(raw_date, datetime)
                            else datetime.fromisoformat(raw_date)
                        )
                        order_ts = order_ts.replace(microsecond=0)

                        restaurant_natural_id = str(doc["restaurant"]["id"])
                        cur.execute(
                            "SELECT id FROM dds.dm_restaurants "
                            "WHERE restaurant_id = %s "
                            "AND active_from <= %s AND active_to > %s "
                            "ORDER BY active_from DESC LIMIT 1;",
                            (restaurant_natural_id, order_ts, order_ts),
                        )
                        restaurant_row = cur.fetchone()
                        if restaurant_row is None:
                            cur.execute(
                                "SELECT id FROM dds.dm_restaurants "
                                "WHERE restaurant_id = %s "
                                "ORDER BY active_from ASC LIMIT 1;",
                                (restaurant_natural_id,),
                            )
                            restaurant_row = cur.fetchone()

                        cur.execute(
                            "SELECT id FROM dds.dm_timestamps WHERE ts = %s;",
                            (order_ts,),
                        )
                        timestamp_row = cur.fetchone()

                        user_natural_id = str(doc["user"]["id"])
                        cur.execute(
                            "SELECT id FROM dds.dm_users WHERE user_id = %s;",
                            (user_natural_id,),
                        )
                        user_row = cur.fetchone()

                        if not restaurant_row:
                            skipped_no_restaurant += 1
                            continue
                        if not timestamp_row:
                            skipped_no_timestamp += 1
                            continue
                        if not user_row:
                            skipped_no_user += 1
                            continue

                        restaurant_sk, timestamp_sk, user_sk = (
                            restaurant_row[0],
                            timestamp_row[0],
                            user_row[0],
                        )

                        cur.execute(
                            "SELECT id FROM dds.dm_orders WHERE order_key = %s;",
                            (order_key,),
                        )
                        existing = cur.fetchone()

                        if existing:
                            cur.execute(
                                "UPDATE dds.dm_orders "
                                "SET order_status = %s, restaurant_id = %s, "
                                "timestamp_id = %s, user_id = %s "
                                "WHERE order_key = %s;",
                                (order_status, restaurant_sk, timestamp_sk, user_sk, order_key),
                            )
                        else:
                            cur.execute(
                                "INSERT INTO dds.dm_orders "
                                "(order_key, order_status, restaurant_id, timestamp_id, user_id) "
                                "VALUES (%s, %s, %s, %s, %s);",
                                (order_key, order_status, restaurant_sk, timestamp_sk, user_sk),
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
                            (WF_KEY_DM_ORDERS, settings_payload),
                        )

                    log.info(
                        "Processed %s stg rows into dds.dm_orders (matched=%s, "
                        "skipped: no_restaurant=%s no_timestamp=%s no_user=%s), cursor moved to %s",
                        len(stg_rows),
                        matched_count,
                        skipped_no_restaurant,
                        skipped_no_timestamp,
                        skipped_no_user,
                        new_last_loaded_ts,
                    )
        finally:
            dwh_conn.close()

    @task(task_id="fct_product_sales_load")
    def load_fct_product_sales():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:  
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM dds.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY_FCT_PRODUCT_SALES,),
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
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_orders "
                            "WHERE update_ts > %s "
                            "ORDER BY update_ts ASC;",
                            (last_loaded_ts,),
                        )
                    else:
                        cur.execute(
                            "SELECT object_value, update_ts "
                            "FROM stg.ordersystem_orders "
                            "ORDER BY update_ts ASC;"
                        )
                    stg_rows = cur.fetchall()

                    if not stg_rows:
                        log.info("No new orders found in stg after %s", last_loaded_ts)
                        return

                    matched_count = 0
                    skipped_no_order = 0
                    skipped_no_product = 0
                    skipped_no_bonus = 0

                    for object_value, _row_update_ts in stg_rows:
                        doc = (
                            object_value
                            if isinstance(object_value, dict)
                            else json.loads(object_value)
                        )
                        order_key = str(doc["_id"])
                        restaurant_natural_id = str(doc["restaurant"]["id"])

                        cur.execute(
                            "SELECT id, restaurant_id FROM dds.dm_orders WHERE order_key = %s;",
                            (order_key,),
                        )
                        order_row = cur.fetchone()
                        if order_row is None:
                            skipped_no_order += 1
                            continue
                        order_sk, restaurant_sk = order_row

                        cur.execute(
                            "SELECT event_value FROM stg.bonussystem_events "
                            "WHERE event_value::jsonb ->> 'order_id' = %s "
                            "ORDER BY id DESC LIMIT 1;",
                            (order_key,),
                        )
                        bonus_row = cur.fetchone()
                        bonus_by_product = {}
                        if bonus_row:
                            bonus_doc = (
                                bonus_row[0]
                                if isinstance(bonus_row[0], dict)
                                else json.loads(bonus_row[0])
                            )
                            for payment in bonus_doc.get("product_payments", []):
                                bonus_by_product[str(payment["product_id"])] = (
                                    payment.get("bonus_payment", 0),
                                    payment.get("bonus_grant", 0),
                                )

                        for item in doc.get("order_items", []):
                            product_natural_id = str(item["id"])
                            count = item["quantity"]
                            price = item["price"]
                            total_sum = float(price) * int(count)

                            cur.execute(
                                "SELECT id FROM dds.dm_products "
                                "WHERE product_id = %s AND restaurant_id = %s AND active_to = %s;",
                                (product_natural_id, restaurant_sk, ACTIVE_TO_INFINITY),
                            )
                            product_row = cur.fetchone()
                            if product_row is None:
                                skipped_no_product += 1
                                continue
                            product_sk = product_row[0]

                            if product_natural_id not in bonus_by_product:
                                skipped_no_bonus += 1
                                continue
                            bonus_payment, bonus_grant = bonus_by_product[product_natural_id]

                            cur.execute(
                                "SELECT id FROM dds.fct_product_sales "
                                "WHERE order_id = %s AND product_id = %s;",
                                (order_sk, product_sk),
                            )
                            existing = cur.fetchone()

                            if existing:
                                cur.execute(
                                    "UPDATE dds.fct_product_sales "
                                    "SET count = %s, price = %s, total_sum = %s, "
                                    "bonus_payment = %s, bonus_grant = %s "
                                    "WHERE id = %s;",
                                    (
                                        count,
                                        price,
                                        total_sum,
                                        bonus_payment,
                                        bonus_grant,
                                        existing[0],
                                    ),
                                )
                            else:
                                cur.execute(
                                    "INSERT INTO dds.fct_product_sales "
                                    "(product_id, order_id, count, price, total_sum, "
                                    "bonus_payment, bonus_grant) "
                                    "VALUES (%s, %s, %s, %s, %s, %s, %s);",
                                    (
                                        product_sk,
                                        order_sk,
                                        count,
                                        price,
                                        total_sum,
                                        bonus_payment,
                                        bonus_grant,
                                    ),
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
                            (WF_KEY_FCT_PRODUCT_SALES, settings_payload),
                        )

                    log.info(
                        "Processed %s stg rows into dds.fct_product_sales (matched=%s, "
                        "skipped: no_order=%s no_product=%s no_bonus=%s), cursor moved to %s",
                        len(stg_rows),
                        matched_count,
                        skipped_no_order,
                        skipped_no_product,
                        skipped_no_bonus,
                        new_last_loaded_ts,
                    )
        finally:
            dwh_conn.close()

    @task(task_id="fct_deliveries_load")
    def load_fct_deliveries():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, workflow_settings "
                        "FROM dds.srv_wf_settings "
                        "WHERE workflow_key = %s;",
                        (WF_KEY_FCT_DELIVERIES,),
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
                        delivery_id = str(doc["delivery_id"])
                        order_key = str(doc["order_id"])
                        courier_natural_id = str(doc["courier_id"])
                        rate = doc["rate"]
                        tip_sum = doc.get("tip_sum", 0)

                        cur.execute(
                            "SELECT id FROM dds.dm_orders WHERE order_key = %s;",
                            (order_key,),
                        )
                        order_row = cur.fetchone()
                        if order_row is None:
                            skipped_no_order += 1
                            continue
                        order_sk = order_row[0]

                        cur.execute(
                            "SELECT id FROM dds.dm_couriers WHERE courier_id = %s;",
                            (courier_natural_id,),
                        )
                        courier_row = cur.fetchone()
                        if courier_row is None:
                            skipped_no_courier += 1
                            continue
                        courier_sk = courier_row[0]

                        cur.execute(
                            "SELECT id FROM dds.fct_deliveries WHERE delivery_id = %s;",
                            (delivery_id,),
                        )
                        existing = cur.fetchone()

                        if existing:
                            cur.execute(
                                "UPDATE dds.fct_deliveries "
                                "SET order_id = %s, courier_id = %s, rate = %s, tip_sum = %s "
                                "WHERE id = %s;",
                                (order_sk, courier_sk, rate, tip_sum, existing[0]),
                            )
                        else:
                            cur.execute(
                                "INSERT INTO dds.fct_deliveries "
                                "(delivery_id, order_id, courier_id, rate, tip_sum) "
                                "VALUES (%s, %s, %s, %s, %s);",
                                (delivery_id, order_sk, courier_sk, rate, tip_sum),
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
                            (WF_KEY_FCT_DELIVERIES, settings_payload),
                        )

                    log.info(
                        "Processed %s stg rows into dds.fct_deliveries (matched=%s, "
                        "skipped: no_order=%s no_courier=%s), cursor moved to %s",
                        len(stg_rows),
                        matched_count,
                        skipped_no_order,
                        skipped_no_courier,
                        new_last_loaded_ts,
                    )
        finally:
            dwh_conn.close()

    load_dm_users()
    load_dm_restaurants()
    load_dm_timestamps()
    load_dm_products()
    load_dm_orders()
    load_fct_product_sales()
    load_fct_deliveries()