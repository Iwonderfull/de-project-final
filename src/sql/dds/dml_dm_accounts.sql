INSERT INTO STV2024021912__DWH.dm_accounts(account_id, account_number, load_dt, load_src)
SELECT 
    HASH(v.account_number) AS account_id, 
    v.account_number, 
    now() AS load_dt, 
    'e-commerce-yapg' AS load_src
FROM (
    SELECT t.account_number_from AS account_number
    FROM STV2024021912__STAGING.transactions t
    WHERE t.transaction_dt BETWEEN '{{since}}' AND '{{before}}'
    AND t.account_number_from > 0
    UNION
    SELECT t.account_number_to AS account_number
    FROM STV2024021912__STAGING.transactions t
    WHERE t.transaction_dt BETWEEN '{{since}}' AND '{{before}}'
    AND t.account_number_to > 0
) v
WHERE NOT EXISTS (
    SELECT 1
    FROM STV2024021912__DWH.dm_accounts a
    WHERE a.account_id = HASH(v.account_number)
);
