import json
import logging
from datetime import datetime
from urllib.parse import quote_plus as quote

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json, execute_values
from pymongo.mongo_client import MongoClient

log = logging.getLogger(__name__)

DWH_CONN_ID = "PG_WAREHOUSE_CONNECTION"
LAST_LOADED_TS_KEY = "last_loaded_ts"

WF_KEY_RESTAURANTS = "ordersystem_restaurants_origin_to_stg_workflow"
WF_KEY_USERS = "ordersystem_users_origin_to_stg_workflow"
WF_KEY_ORDERS = "ordersystem_orders_origin_to_stg_workflow"


class MongoConnect:
    def __init__(self, cert_path, user, pw, host, rs, auth_db, main_db):
        self.user = user
        self.pw = pw
        self.hosts = [host]
        self.replica_set = rs
        self.auth_db = auth_db
        self.main_db = main_db
        self.cert_path = cert_path

    def url(self):
        return "mongodb://{user}:{pw}@{hosts}/?replicaSet={rs}&authSource={auth_src}".format(
            user=quote(self.user),
            pw=quote(self.pw),
            hosts=",".join(self.hosts),
            rs=self.replica_set,
            auth_src=self.auth_db,
        )

    def client(self):
        return MongoClient(self.url(), tlsCAFile=self.cert_path)[self.main_db]


def load_collection_incremental(collection_name: str, target_table: str, workflow_key: str):
    """
    Общая логика инкрементальной загрузки коллекции MongoDB в staging-таблицу:
    читаем курсор (update_ts) из stg.srv_wf_settings, забираем документы новее
    курсора, сохраняем их as is и сдвигаем курсор — всё в одной транзакции.
    """
    cert_path = Variable.get("MONGO_DB_CERTIFICATE_PATH")
    db_user = Variable.get("MONGO_DB_USER")
    db_pw = Variable.get("MONGO_DB_PASSWORD")
    rs = Variable.get("MONGO_DB_REPLICA_SET")
    db = Variable.get("MONGO_DB_DATABASE_NAME")
    host = Variable.get("MONGO_DB_HOST")

    mongo_connect = MongoConnect(cert_path, db_user, db_pw, host, rs, db, db)
    collection = mongo_connect.client().get_collection(collection_name)

    dwh_conn = PostgresHook(DWH_CONN_ID).get_conn()
    try:
        with dwh_conn:  
            with dwh_conn.cursor() as cur:
                cur.execute(
                    "SELECT id, workflow_settings "
                    "FROM stg.srv_wf_settings "
                    "WHERE workflow_key = %s;",
                    (workflow_key,),
                )
                wf_row = cur.fetchone()
                wf_id, last_loaded_ts = None, None
                if wf_row:
                    wf_id, wf_settings = wf_row
                    raw_ts = wf_settings.get(LAST_LOADED_TS_KEY)
                    if raw_ts:
                        last_loaded_ts = datetime.fromisoformat(raw_ts)

                mongo_filter = (
                    {"update_ts": {"$gt": last_loaded_ts}} if last_loaded_ts else {}
                )
                docs = list(
                    collection.find(
                        filter=mongo_filter,
                        sort=[("update_ts", 1)]
                    )
                )

                if not docs:
                    log.info("No new %s found after %s", collection_name, last_loaded_ts)
                    return

                rows = [
                    (str(doc["_id"]), json.dumps(doc, default=str), doc["update_ts"])
                    for doc in docs
                ]
                execute_values(
                    cur,
                    f"INSERT INTO {target_table} (object_id, object_value, update_ts) "
                    f"VALUES %s "
                    f"ON CONFLICT (object_id) DO UPDATE "
                    f"SET object_value = EXCLUDED.object_value, "
                    f"update_ts = EXCLUDED.update_ts;",
                    rows,
                )

                new_last_loaded_ts = docs[-1]["update_ts"]
                settings_payload = Json({LAST_LOADED_TS_KEY: new_last_loaded_ts.isoformat()})

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
                        (workflow_key, settings_payload),
                    )

                log.info(
                    "Loaded %s rows into %s, cursor moved to %s",
                    len(rows),
                    target_table,
                    new_last_loaded_ts,
                )
    finally:
        dwh_conn.close()


with DAG(
    dag_id="stg_order_system_dag",
    schedule_interval="*/15 * * * *",
    start_date=datetime(2022, 1, 1),
    catchup=False,
    is_paused_upon_creation=False,
    tags=["stg", "ordersystem", "origin", "mongo"],
) as dag:

    @task(task_id="restaurants_load")
    def load_restaurants():
        load_collection_incremental(
            "restaurants", "stg.ordersystem_restaurants", WF_KEY_RESTAURANTS
        )

    @task(task_id="users_load")
    def load_users():
        load_collection_incremental("users", "stg.ordersystem_users", WF_KEY_USERS)

    @task(task_id="orders_load")
    def load_orders():
        load_collection_incremental("orders", "stg.ordersystem_orders", WF_KEY_ORDERS)

    load_restaurants()
    load_users()
    load_orders()