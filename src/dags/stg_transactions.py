"""\
Загружает данные о транзакциях из источника в Вертику

### Соединения
- `e-commerce-yapg (Postgres)` - соединение с постгресом
- `e-commerce-vertica (Vertica)` - сокдинение с вертикой

### Переменные
- `stage-sql-path (str)` - путь к папке с sql-скриптами

### Зависимости
- pydantic
- typing_extensions=4.7.1

### Провайдеры
- vertica
- postgres
"""

import csv
from pathlib import Path
from datetime import datetime

import pendulum
from pydantic import BaseModel
from psycopg2.extensions import cursor as Psycopg2Cursor
from vertica_python import Connection as VerticaConnection
from vertica_python.vertica.cursor import Cursor as VerticaCursor

# from psycopg2.extras import DictRow #  hook.get_conn().cursor(cursor_factory=DictRow) вызывает странную ошибку

from jinja2 import TemplateNotFound
from airflow import DAG
from airflow.models import Variable, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.vertica.operators.vertica import (
    VerticaOperator,
)  # нет подстановки параметров ни через parameters, ни через params
from airflow.exceptions import AirflowSkipException


def _upload_transactions_to_vertica(
    vertica_conn_id: str,
    sql: str,
    table_name: str,
    local_path: str,
    ti: TaskInstance,
    dag: DAG,
) -> None:
    if not local_path or local_path == "None":
        raise AirflowSkipException("No data available.")
    hook = VerticaHook(vertica_conn_id)
    ti.log.info("Costruct query with param: %s", local_path)
    try:
        env = dag.get_template_env()
        template = env.get_template(sql)
        query = template.render(local_path=local_path)
    except TemplateNotFound:
        query = sql
    ti.log.info("Upload to Vertica: %s", local_path)
    # hook.bulk_load(table_name, local_path) # Not implemented Error ))
    conn: VerticaConnection = hook.get_conn()
    cursor: VerticaCursor = conn.cursor()
    ti.log.info("Using query:\n%s", query)
    cursor.execute(query)
    ti.log.info("Rows upload: %s", cursor.fetchall())
    conn.commit()
    cursor.close()


def _donwload_transactions(
    conn_id: str,
    local_path: str,
    sql: str,
    days_interval: int,
    dag: DAG,
    ti: TaskInstance,
    logical_date,
) -> str:
    class Transaction(BaseModel):
        operation_id: str
        account_number_from: int
        account_number_to: int
        currency_code: int
        country: str
        status: str
        transaction_type: str
        amount: int
        transaction_dt: datetime

    hook = PostgresHook(conn_id)

    # Construct query
    date_since = logical_date.to_date_string()
    date_before = logical_date.add(days=days_interval).to_date_string()
    ti.log.info("Costruct query with for %s days since: %s", days_interval, date_since)
    try:
        env = dag.get_template_env()
        template = env.get_template(sql)
        query = template.render(since=date_since, before=date_before)
    except TemplateNotFound:
        query = sql

    # Execute query
    ti.log.info(
        "Get data from transactions since: %s and before: %s", date_since, date_before
    )
    cursor: Psycopg2Cursor = hook.get_cursor()
    cursor.execute(query)
    ti.log.info("Affected %s rows", cursor.rowcount)
    if not cursor.rowcount:
        cursor.close()
        raise AirflowSkipException("No data available.")

    # Get field names
    field_names = [desc[0] for desc in cursor.description]
    ti.log.info("Write data to file: %s", local_path)
    # Write rows to file
    local_path = Path(local_path)
    with local_path.open("w") as fp:
        writer = csv.DictWriter(fp, fieldnames=field_names, lineterminator="\n")
        # Write witout header
        for row in cursor:
            transaction = Transaction(
                **{name: value for name, value in zip(field_names, row)}
            )
            writer.writerow(transaction.model_dump(mode="json"))
    cursor.close()
    return local_path.resolve().as_posix()


with DAG(
    dag_id="stage-transactions",
    description="Загружает данные из хранилища",
    schedule="@daily",
    start_date=pendulum.from_format("01-10-2022", "DD-MM-YYYY"),
    end_date=pendulum.from_format("02-11-2022", "DD-MM-YYYY"),
    template_searchpath=Variable.get("stage-sql-path", "/lessons/sql/stg"),
    default_args={"owner": "badun61"},
    catchup=True,
    doc_md=__doc__,
    is_paused_upon_creation=True,
) as dag:
    ensure_tables = VerticaOperator(
        task_id="ensure_tables",
        vertica_conn_id="e-commerce-vertica",
        sql="ddl.sql",
    )

    donwload_transactions = PythonOperator(
        task_id="donwload_transactions",
        python_callable=_donwload_transactions,
        op_kwargs={
            "conn_id": "e-commerce-yapg",
            "local_path": "/lessons/data/transactions_{{ ds }}.csv",
            "sql": "transactions_from.sql",
            "days_interval": 1,
        },
    )

    upload_transactions_to_vertica = PythonOperator(
        task_id="upload_transactions_to_vertica",
        python_callable=_upload_transactions_to_vertica,
        op_kwargs={
            "vertica_conn_id": "e-commerce-vertica",
            "sql": "transactions_to.sql",
            "table_name": "STV2024021912__STAGING.transactions",
            "local_path": '{{ti.xcom_pull(task_ids="donwload_transactions")}}',
        },
    )

    ensure_tables >> upload_transactions_to_vertica
    donwload_transactions >> upload_transactions_to_vertica
