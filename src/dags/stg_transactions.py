"""\
Загружает данные из источника 

### Соединения
- 

### Переменные
- 
"""

import csv
from pathlib import Path
from typing import Any
import pendulum
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.providers.vertica.operators.vertica import VerticaOperator
from airflow.utils.log.logging_mixin import LoggingMixin


class VerticaHandler(LoggingMixin):
    "Transfering data to veritca"

    def __init__(self, conn_id: str, target_table: str, local_path: str) -> None:
        self._conn_id = conn_id
        self._local_path = Path(local_path)
        self._target_table = target_table

    def __call__(self, cursor):
        vertica_hook = VerticaHook(self._conn_id)
        # vertica_hook.bulk_load(transactions)


class LocalWriter(LoggingMixin):
    def __init__(
        self,
        local_path,
    ) -> None:
        self.local_path = Path(local_path)

    def __call__(self, cursor) -> Any:
        self.log.info("Usign local path: %s", self.local_path)
        with self.local_path.open("w") as fp:
            writer = csv.writer(fp)
            for row in cursor:
                self.log.info("write row: %s", row)
                writer.writerow(row)


def get_transactions(conn_id: str, local_path: str, dag: DAG, logical_date):
    hook = PostgresHook(conn_id)
    env = dag.get_template_env()
    template = env.get_template("transactions_from.sql")
    date = logical_date.to_date_string()
    query = template.render(since=date)
    print("Get a day of transactions since:", date)
    cursor = hook.get_cursor()
    cursor.execute(query)
    with Path(local_path).open("w") as fp:
        for row in cursor:
            fp.write(",".join(map(str, row)))


with DAG(
    dag_id="stage-transactions",
    description="Загружает данные из хранилища",
    schedule="@daily",
    start_date=pendulum.from_format("01-10-2022", "DD-MM-YYYY"),
    end_date=pendulum.from_format("02-11-2022", "DD-MM-YYYY"),
    template_searchpath=Variable.get("e-commerce-stage-sql", "/lessons/sql/stg"),
    default_args={"owner": "badun61"},
    catchup=True,
    doc_md=__doc__,
    is_paused_upon_creation=True,
) as dag:
    # ensure_tables = VerticaOperator(
    #     task_id="ensure_tables",
    #     vertica_conn_id="e-commerce-vertica",
    #     sql="ddl.sql",
    # )

    # load_transactions = SQLExecuteQueryOperator(
    #     task_id="load_transactions",
    #     sql_conn_id="e-commerce-yapg",
    #     # handler=VerticaHandler("e-commerce-vertica"),
    #     sql="transactions_from.sql",
    #     parameters={
    #         "since": "{{ dag_run.logical_date }}",
    #         "before": "{{ dag_run.logical_date }}",
    #         "local_path": ...
    #     },
    # )

    # load_transactions = SQLExecuteQueryOperator(
    #     task_id="load_transactions",
    #     conn_id="e-commerce-yapg",
    #     handler=LocalWriter("/lessons/data/transactions_{{ ds }}.csv"),
    #     sql="transactions_from.sql",
    #     parameters={
    #         "since": "{{ ds }}",
    #         # "local_path": "/lessons/data/transactions_{{ds}}.csv",
    #     },
    #     # do_xcom_push=False,
    # )

    success = PythonOperator(
        task_id="get_transactions",
        python_callable=get_transactions,
        op_kwargs={
            "conn_id": "e-commerce-yapg",
            "local_path": "/lessons/data/transactions_{{ ds }}.csv",
        },
    )

    # (ensure_tables >> load_transactions)
