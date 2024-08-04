CREATE TABLE IF NOT EXISTS STV2024021912__DWH.global_metrics (
    date_update date not null,
    currency_from int not null,
    amount_total numeric(16, 2) not null,
    cnt_transactions int not null,
    avg_transactions_per_account numeric(10, 2) not null,
    cnt_accounts_make_transactions int not null,
    CONSTRAINT global_metrics_pk PRIMARY KEY(date_update, currency_from) ENABLED
)
ORDER BY date_update, currency_from
SEGMENTED BY hash(date_update, currency_from) all nodes
PARTITION BY date_update
GROUP BY calendar_hierarchy_day(date_update, 3, 2);

CREATE TABLE IF NOT EXISTS STV2024021912__DWH.global_metrics_copy
  LIKE STV2024021912__DWH.global_metrics INCLUDING PROJECTIONS;