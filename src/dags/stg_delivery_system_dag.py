import logging
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

log = logging.getLogger(__name__)

DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"

API_BASE_URL = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net"
API_KEY = "25c27781-8fde-4b30-a22e-524044a7580f"
API_NICKNAME = "Sappy"
API_COHORT = "14"
PAGE_LIMIT = 50
LOOKBACK_DAYS = 7


def _api_headers():
    return {
        "X-Nickname": API_NICKNAME,
        "X-Cohort": API_COHORT,
        "X-API-KEY": API_KEY,
    }


def _fetch_all_pages(path, sort_field, extra_params=None):
    results = []
    offset = 0
    while True:
        params = {
            "sort_field": sort_field,
            "sort_direction": "asc",
            "limit": PAGE_LIMIT,
            "offset": offset,
        }
        if extra_params:
            params.update(extra_params)

        response = requests.get(
            f"{API_BASE_URL}{path}",
            headers=_api_headers(),
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        page = response.json()

        if not page:
            break

        results.extend(page)
        offset += PAGE_LIMIT

        if len(page) < PAGE_LIMIT:
            break

    return results


with DAG(
    dag_id="stg_delivery_system_dag",
    schedule_interval="@daily",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["stg", "delivery-system"],
) as dag:

    @task(task_id="couriers_load")
    def load_couriers():
        couriers = _fetch_all_pages("/couriers", sort_field="id")
        if not couriers:
            log.info("No couriers returned by API")
            return

        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    for courier in couriers:
                        cur.execute(
                            "INSERT INTO stg.deliverysystem_couriers "
                            "(object_id, object_value, load_dttm) "
                            "VALUES (%s, %s, now()) "
                            "ON CONFLICT (object_id) DO UPDATE SET "
                            "object_value = EXCLUDED.object_value, "
                            "load_dttm = EXCLUDED.load_dttm;",
                            (courier["_id"], Json(courier)),
                        )
            log.info("couriers_load: upserted %s couriers", len(couriers))
        finally:
            dwh_conn.close()

    @task(task_id="deliveries_load")
    def load_deliveries():
        to_dt = datetime.utcnow()
        from_dt = to_dt - timedelta(days=LOOKBACK_DAYS)

        deliveries = _fetch_all_pages(
            "/deliveries",
            sort_field="_id",
            extra_params={
                "from": from_dt.strftime("%Y-%m-%d %H:%M:%S"),
                "to": to_dt.strftime("%Y-%m-%d %H:%M:%S"),
            },
        )
        if not deliveries:
            log.info("No deliveries returned by API for the last %s days", LOOKBACK_DAYS)
            return

        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:
                with dwh_conn.cursor() as cur:
                    for delivery in deliveries:
                        cur.execute(
                            "INSERT INTO stg.deliverysystem_deliveries "
                            "(object_id, object_value, load_dttm) "
                            "VALUES (%s, %s, now()) "
                            "ON CONFLICT (object_id) DO UPDATE SET "
                            "object_value = EXCLUDED.object_value, "
                            "load_dttm = EXCLUDED.load_dttm;",
                            (delivery["delivery_id"], Json(delivery)),
                        )
            log.info(
                "deliveries_load: upserted %s deliveries (window %s .. %s)",
                len(deliveries),
                from_dt,
                to_dt,
            )
        finally:
            dwh_conn.close()

    load_couriers()
    load_deliveries()