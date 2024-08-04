CREATE SCHEMA IF NOT EXISTS STV2024021912__DWH;

CREATE TABLE IF NOT EXISTS STV2024021912__DWH.dm_currencies (
    currency_id int not null PRIMARY KEY ENABLED,
    currency_code int not null,
    load_dt timestamp not null,
    load_src varchar(20) not null
)
ORDER BY currency_code
UNSEGMENTED ALL NODES;

CREATE TABLE IF NOT EXISTS STV2024021912__DWH.dm_accounts (
    account_id int not null PRIMARY KEY ENABLED,
    account_number int not null,
    load_dt timestamp not null,
    load_src varchar(20) not null
)
ORDER BY account_number
SEGMENTED BY HASH(account_number) ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV2024021912__DWH.dm_trans_types (
    trans_type_id int not null PRIMARY KEY ENABLED,
    trans_type varchar(30) not null,
    load_dt timestamp not null,
    load_src varchar(20) not null
)
ORDER BY trans_type
UNSEGMENTED ALL NODES;

CREATE TABLE IF NOT EXISTS STV2024021912__DWH.dm_transactions (
    transaction_id int not null PRIMARY KEY ENABLED,
    operation_id varchar(60) not null,
    trans_type_id int not null REFERENCES STV2024021912__DWH.dm_trans_types(trans_type_id),
    trans_start_ts timestamp not null,
    load_dt timestamp not null,
    load_src varchar(20) not null
)
ORDER BY operation_id
SEGMENTED BY HASH(operation_id) ALL NODES
PARTITION BY load_dt::date
GROUP BY calendar_hierarchy_day(load_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV2024021912__DWH.dm_countries (
    country_id int not null PRIMARY KEY ENABLED,
    country_name varchar(30) not null,
    load_dt timestamp not null,
    load_src varchar(20) not null
)
ORDER BY country_name
UNSEGMENTED ALL NODES;

CREATE TABLE IF NOT EXISTS STV2024021912__DWH.fct_currency_exchange (
    id int not null primary key enabled,
    currency_id int not null REFERENCES STV2024021912__DWH.dm_currencies(currency_id),
    currency_id_with int not null REFERENCES STV2024021912__DWH.dm_currencies(currency_id),
    update_dt timestamp not null,
    currency_with_div numeric(5, 3) not null,
    load_dt timestamp not null,
    load_src varchar(20) not null
)
ORDER BY update_dt
SEGMENTED BY HASH(currency_id, currency_id_with, update_dt) ALL NODES
PARTITION BY update_dt::date
GROUP BY calendar_hierarchy_day(update_dt::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV2024021912__DWH.fct_trans_amount_status (
    id int not null primary key enabled,
    transaction_id int not null REFERENCES STV2024021912__DWH.dm_transactions(transaction_id),
    account_from int not null REFERENCES STV2024021912__DWH.dm_accounts(account_id),
    account_to int not null REFERENCES STV2024021912__DWH.dm_accounts(account_id),
    currency_id int not null REFERENCES STV2024021912__DWH.dm_currencies(currency_id),
    country_id int not null REFERENCES STV2024021912__DWH.dm_countries(country_id),
    transaction_dt timestamp not null,
    amount int not null,
    transaction_status varchar(30),
    load_dt timestamp not null,
    load_src varchar(20) not null
)
ORDER BY transaction_dt, transaction_status
SEGMENTED BY HASH(transaction_id, account_from, account_to, currency_id, transaction_status) ALL NODES
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);