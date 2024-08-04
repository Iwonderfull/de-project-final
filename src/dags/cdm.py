"""\
Загружает данные слоя витрин

### Соединения
- `e-commerce-vertica (Vertica)` - соединение с вертикой

### Переменные
- `cdm-sql-path (str)` - путь к папке с sql-скриптами

### Зависимости
- vertica_python

### Провайдеры
- vertica
- postgres
"""

from typing import Tuple
import pendulum
from vertica_python import Connection as VerticaConnection
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.utils.log.logging_mixin import LoggingMixin


class CDMFuncitons(LoggingMixin):
    def __init__(
        self,
        vertica_conn_id,
        dag: DAG,
    ) -> None:
        self.vertica_conn_id = vertica_conn_id
        self.dag = dag

    def fill_interval(self, template_name: str, days_interval: int, logical_date):
        self.log.info("Using tamplate: %s", template_name)
        since, before = self.get_interval(logical_date=logical_date, days=days_interval)
        self.log.info("Set dates for query since %s and before %s", since, before)
        self.execute_query(template_name, since=since, before=before)

    def fill(self, template_name: str, logical_date):
        self.log.info("Using tamplate: %s", template_name)
        since = logical_date.to_date_string()
        self.log.info("Set dates for query since %s", since)
        self.execute_query(template_name, since=since)

    def ensure_tables(self):
        self.log.info("Ensure that tables exists or create")
        template_name = "ddl.sql"
        self.execute_query(template_name)

    def execute_query(self, sql, **kwargs):
        hook = VerticaHook(self.vertica_conn_id)
        query = self.render_query(sql, **kwargs)
        self.log.info("Using query:\n%s", query)
        conn: VerticaConnection = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(query)
        self.log.info("Using results: %s", cursor.fetchall())
        conn.commit()
        cursor.close()

    @staticmethod
    def get_interval(logical_date, days: int) -> Tuple:
        date_since = logical_date.to_date_string()
        date_before = logical_date.add(days=days).to_date_string()
        return date_since, date_before

    def render_query(self, template_name: str, **kwargs):
        env = self.dag.get_template_env()
        template = env.get_template(template_name)
        query = template.render(**kwargs)
        return query


with DAG(
    dag_id="cdm",
    description="Загружает данные витрин",
    schedule="@daily",
    start_date=pendulum.from_format("01-10-2022", "DD-MM-YYYY"),
    end_date=pendulum.from_format("02-11-2022", "DD-MM-YYYY"),
    template_searchpath=Variable.get(
        "dds-sql-path",
        "/lessons/sql/cdm",
    ),
    default_args={"owner": "badun61"},
    catchup=True,
    doc_md=__doc__,
    is_paused_upon_creation=True,
) as dag:
    cdm_func = CDMFuncitons(
        vertica_conn_id="e-commerce-vertica",
        dag=dag,
    )

    ensure_tables = PythonOperator(
        task_id="ensure_tables", python_callable=cdm_func.ensure_tables
    )

    insert_fake_row = PythonOperator(
        task_id="insert_fake_row",
        python_callable=cdm_func.fill,
        op_kwargs={
            "template_name": "insert_fake_row_global_metrics.sql",
        },
    )

    copy_drop_partitions = PythonOperator(
        task_id="copy_drop_partitions",
        python_callable=cdm_func.fill,
        op_kwargs={
            "template_name": "copy_drop_partitions_global_metrics.sql",
        },
    )

    fill_global_metrics = PythonOperator(
        task_id="fill_global_metrics",
        python_callable=cdm_func.fill_interval,
        op_kwargs={
            "template_name": "fill_global_metrics.sql",
            "days_interval": 1,
        },
    )

    swap_partitions = PythonOperator(
        task_id="swap_partitions",
        python_callable=cdm_func.fill,
        op_kwargs={
            "template_name": "swap_partitions_global_metrics.sql",
        },
    )

    (
        ensure_tables
        >> insert_fake_row
        >> copy_drop_partitions
        >> fill_global_metrics
        >> swap_partitions
    )
