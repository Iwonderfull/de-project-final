"""\
Загружает данные детального слоя

### Соединения
- `e-commerce-vertica (Vertica)` - соединение с вертикой

### Переменные
- `dds-sql-path (str)` - путь к папке с sql-скриптами

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
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.vertica.hooks.vertica import VerticaHook
from airflow.utils.log.logging_mixin import LoggingMixin


class DDSFuncitons(LoggingMixin):
    def __init__(
        self,
        vertica_conn_id,
        dag: DAG,
    ) -> None:
        self.vertica_conn_id = vertica_conn_id
        self.dag = dag

    def fill(self, table, days_interval, logical_date):
        self.log.info("Fill table: %s", table)
        template_name = f"dml_{table}.sql"
        since, before = self.get_interval(logical_date=logical_date, days=days_interval)
        self.log.info("Set dates for query since %s and before %s", since, before)
        self.execute_query(template_name, since=since, before=before)

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
    dag_id="dds",
    description="Загружает детальные данные",
    schedule="@daily",
    start_date=pendulum.from_format("01-10-2022", "DD-MM-YYYY"),
    end_date=pendulum.from_format("02-11-2022", "DD-MM-YYYY"),
    template_searchpath=Variable.get(
        "dds-sql-path",
        "/lessons/sql/dds",
    ),
    default_args={"owner": "badun61"},
    catchup=True,
    doc_md=__doc__,
    is_paused_upon_creation=True,
) as dag:
    dds_func = DDSFuncitons(
        vertica_conn_id="e-commerce-vertica",
        dag=dag,
    )

    ensure_tables = PythonOperator(
        task_id="ensure_tables",
        python_callable=dds_func.ensure_tables,
    )

    with TaskGroup(group_id="fill_first_dms") as fill_first_dms_group:

        fill_dm_accounts = PythonOperator(
            task_id="fill_dm_accounts",
            python_callable=dds_func.fill,
            op_kwargs={
                "table": "dm_accounts",
                "days_interval": 1,
            },
        )
        if not fill_first_dms_group.has_task(fill_dm_accounts):
            fill_first_dms_group.add(fill_dm_accounts)

        fill_dm_countries = PythonOperator(
            task_id="fill_dm_countries",
            python_callable=dds_func.fill,
            op_kwargs={
                "table": "dm_countries",
                "days_interval": 1,
            },
        )
        if not fill_first_dms_group.has_task(fill_dm_countries):
            fill_first_dms_group.add(fill_dm_countries)

        fill_dm_currencies = PythonOperator(
            task_id="fill_dm_currencies",
            python_callable=dds_func.fill,
            op_kwargs={
                "table": "dm_currencies",
                "days_interval": 1,
            },
        )
        if not fill_first_dms_group.has_task(fill_dm_currencies):
            fill_first_dms_group.add(fill_dm_currencies)

        fill_dm_trans_types = PythonOperator(
            task_id="fill_dm_trans_types",
            python_callable=dds_func.fill,
            op_kwargs={
                "table": "dm_trans_types",
                "days_interval": 1,
            },
        )
        if not fill_first_dms_group.has_task(fill_dm_trans_types):
            fill_first_dms_group.add(fill_dm_trans_types)

    fill_dm_transactions = PythonOperator(
        task_id="fill_dm_transactions",
        python_callable=dds_func.fill,
        op_kwargs={
            "table": "dm_transactions",
            "days_interval": 1,
        },
    )

    with TaskGroup(group_id="fill_facts") as fill_facts_group:
        fill_fct_currency_exchange = PythonOperator(
            task_id="fill_fct_currency_exchange",
            python_callable=dds_func.fill,
            op_kwargs={
                "table": "fct_currency_exchange",
                "days_interval": 1,
            },
        )
        if not fill_facts_group.has_task(fill_fct_currency_exchange):
            fill_facts_group.add(fill_fct_currency_exchange)

        fill_fct_trans_amount_status = PythonOperator(
            task_id="fill_fct_trans_amount_status",
            python_callable=dds_func.fill,
            op_kwargs={
                "table": "fct_trans_amount_status",
                "days_interval": 1,
            },
        )
        if not fill_facts_group.has_task(fill_fct_trans_amount_status):
            fill_facts_group.add(fill_fct_trans_amount_status)

    end = EmptyOperator(task_id="end")

    (
        ensure_tables
        >> fill_first_dms_group
        >> fill_dm_transactions
        >> fill_facts_group
        >> end
    )
