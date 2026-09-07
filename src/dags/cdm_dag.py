import logging
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"

COURIER_RATE_TIERS = (
    (4.9, 0.10, 200),
    (4.5, 0.08, 175),
    (4.0, 0.07, 150),
    (0.0, 0.05, 100),
)


def _courier_tier(rate_avg):
    for threshold, percent, min_payment in COURIER_RATE_TIERS:
        if rate_avg >= threshold:
            return percent, min_payment
    return COURIER_RATE_TIERS[-1][1], COURIER_RATE_TIERS[-1][2]


with DAG(
    dag_id="cdm_dag",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["cdm", "dds-to-cdm"],
) as dag:

    @task(task_id="dm_settlement_report_load")
    def load_dm_settlement_report():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:  
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO cdm.dm_settlement_report ("
                        "    restaurant_id, restaurant_name, settlement_date, orders_count, "
                        "    orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, "
                        "    order_processing_fee, restaurant_reward_sum"
                        ") "
                        "SELECT "
                        "    r.restaurant_id, "
                        "    r.restaurant_name, "
                        "    t.ts::date AS settlement_date, "
                        "    COUNT(DISTINCT o.id) AS orders_count, "
                        "    SUM(f.total_sum) AS orders_total_sum, "
                        "    SUM(f.bonus_payment) AS orders_bonus_payment_sum, "
                        "    SUM(f.bonus_grant) AS orders_bonus_granted_sum, "
                        "    SUM(f.total_sum) * 0.25 AS order_processing_fee, "
                        "    SUM(f.total_sum) - SUM(f.bonus_payment) - SUM(f.total_sum) * 0.25 "
                        "        AS restaurant_reward_sum "
                        "FROM dds.fct_product_sales AS f "
                        "JOIN dds.dm_orders AS o ON o.id = f.order_id "
                        "JOIN dds.dm_timestamps AS t ON t.id = o.timestamp_id "
                        "JOIN dds.dm_restaurants AS r ON r.id = o.restaurant_id "
                        "WHERE o.order_status = 'CLOSED' "
                        "GROUP BY r.restaurant_id, r.restaurant_name, t.ts::date "
                        "ON CONFLICT (restaurant_id, settlement_date) DO UPDATE SET "
                        "    restaurant_name = EXCLUDED.restaurant_name, "
                        "    orders_count = EXCLUDED.orders_count, "
                        "    orders_total_sum = EXCLUDED.orders_total_sum, "
                        "    orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum, "
                        "    orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum, "
                        "    order_processing_fee = EXCLUDED.order_processing_fee, "
                        "    restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;"
                    )
                    log.info(
                        "dm_settlement_report_load: upserted %s (restaurant, date) rows",
                        cur.rowcount,
                    )
        finally:
            dwh_conn.close()

    @task(task_id="dm_courier_ledger_load")
    def load_dm_courier_ledger():
        dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
        try:
            with dwh_conn:  
                with dwh_conn.cursor() as cur:
                    cur.execute(
                        "SELECT "
                        "    c.courier_id, "
                        "    c.courier_name, "
                        "    t.year AS settlement_year, "
                        "    t.month AS settlement_month, "
                        "    o.id AS order_sk, "
                        "    d.rate AS order_rate, "
                        "    d.tip_sum AS order_tip_sum, "
                        "    SUM(f.total_sum) AS order_total_sum "
                        "FROM dds.fct_product_sales AS f "
                        "JOIN dds.dm_orders AS o ON o.id = f.order_id "
                        "JOIN dds.dm_timestamps AS t ON t.id = o.timestamp_id "
                        "JOIN dds.fct_deliveries AS d ON d.order_id = o.id "
                        "JOIN dds.dm_couriers AS c ON c.id = d.courier_id "
                        "WHERE o.order_status = 'CLOSED' "
                        "GROUP BY c.courier_id, c.courier_name, t.year, t.month, "
                        "         o.id, d.rate, d.tip_sum;"
                    )
                    order_rows = cur.fetchall()

                    if not order_rows:
                        log.info("dm_courier_ledger_load: no closed+delivered orders found")
                        return

                    groups = {}
                    for (
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                        _order_sk,
                        order_rate,
                        order_tip_sum,
                        order_total_sum,
                    ) in order_rows:
                        key = (courier_id, courier_name, settlement_year, settlement_month)
                        group = groups.setdefault(
                            key,
                            {"orders": [], "rates": [], "tips": 0.0, "orders_total_sum": 0.0},
                        )
                        group["orders"].append(float(order_total_sum))
                        group["rates"].append(int(order_rate))
                        group["tips"] += float(order_tip_sum)
                        group["orders_total_sum"] += float(order_total_sum)

                    for (
                        courier_id,
                        courier_name,
                        settlement_year,
                        settlement_month,
                    ), group in groups.items():
                        rate_avg = sum(group["rates"]) / len(group["rates"])
                        percent, min_payment = _courier_tier(rate_avg)

                        courier_order_sum = sum(
                            max(order_total * percent, min_payment)
                            for order_total in group["orders"]
                        )
                        orders_total_sum = group["orders_total_sum"]
                        orders_count = len(group["orders"])
                        courier_tips_sum = group["tips"]
                        order_processing_fee = orders_total_sum * 0.25
                        courier_reward_sum = courier_order_sum + courier_tips_sum * 0.95

                        cur.execute(
                            "INSERT INTO cdm.dm_courier_ledger ("
                            "    courier_id, courier_name, settlement_year, settlement_month, "
                            "    orders_count, orders_total_sum, rate_avg, order_processing_fee, "
                            "    courier_order_sum, courier_tips_sum, courier_reward_sum"
                            ") "
                            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
                            "ON CONFLICT (courier_id, settlement_year, settlement_month) "
                            "DO UPDATE SET "
                            "    courier_name = EXCLUDED.courier_name, "
                            "    orders_count = EXCLUDED.orders_count, "
                            "    orders_total_sum = EXCLUDED.orders_total_sum, "
                            "    rate_avg = EXCLUDED.rate_avg, "
                            "    order_processing_fee = EXCLUDED.order_processing_fee, "
                            "    courier_order_sum = EXCLUDED.courier_order_sum, "
                            "    courier_tips_sum = EXCLUDED.courier_tips_sum, "
                            "    courier_reward_sum = EXCLUDED.courier_reward_sum;",
                            (
                                courier_id,
                                courier_name,
                                settlement_year,
                                settlement_month,
                                orders_count,
                                orders_total_sum,
                                rate_avg,
                                order_processing_fee,
                                courier_order_sum,
                                courier_tips_sum,
                                courier_reward_sum,
                            ),
                        )

                    log.info(
                        "dm_courier_ledger_load: upserted %s (courier, month) rows",
                        len(groups),
                    )
        finally:
            dwh_conn.close()

    load_dm_settlement_report()
    load_dm_courier_ledger()
