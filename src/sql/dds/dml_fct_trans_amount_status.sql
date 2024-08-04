INSERT INTO STV2024021912__DWH.fct_trans_amount_status(
    id, 
    transaction_id, 
    account_from, 
    account_to, 
    currency_id, 
    country_id, 
    transaction_dt,amount, 
    transaction_status, 
    load_dt, 
    load_src
)
SELECT 
    HASH(d.transaction_id, af.account_id, at.account_id, c.currency_id, t.transaction_status) AS id, 
    d.transaction_id, 
    af.account_id AS account_from, 
    at.account_id AS account_to, 
    c.currency_id, 
    dc.country_id, 
    t.transaction_dt, 
    t.amount, 
    t.transaction_status, 
    now() AS load_dt, 
    'e-commerce-yapg' AS load_src
FROM (
    SELECT DISTINCT 
        t.operation_id, 
        t.account_number_from, 
        t.account_number_to, 
        t.transaction_dt, 
        t.currency_code, 
        t.country,
        t.status AS transaction_status, 
        t.amount
    FROM STV2024021912__STAGING.transactions t
    WHERE t.account_number_from > 0
    AND t.account_number_to > 0
    AND t.transaction_dt BETWEEN '{{since}}' AND '{{before}}'
) t
JOIN STV2024021912__DWH.dm_transactions d
ON d.operation_id = t.operation_id
JOIN STV2024021912__DWH.dm_accounts af
ON af.account_number = t.account_number_from
JOIN STV2024021912__DWH.dm_accounts at
ON at.account_number = t.account_number_to
JOIN STV2024021912__DWH.dm_currencies c
ON c.currency_code = t.currency_code
JOIN STV2024021912__DWH.dm_countries dc
ON dc.country_name = t.country 
WHERE NOT EXISTS (
    SELECT 1
    FROM STV2024021912__DWH.fct_trans_amount_status fta
    WHERE fta.id = HASH(d.transaction_id, af.account_id, at.account_id, c.currency_id, t.transaction_status)
);
