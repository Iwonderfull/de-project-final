INSERT INTO STV2024021912__DWH.dm_currencies(currency_id, currency_code, load_dt, load_src)
SELECT HASH(v.currency_code) AS currency_id, 
    v.currency_code, 
    now() AS load_dt, 
    'e-commerce-yapg' AS load_src
  FROM (
    SELECT c.currency_code
    FROM STV2024021912__STAGING.currencies c
    WHERE c.date_update = '{{since}}'
    UNION
    SELECT c.currency_code_with
    FROM STV2024021912__STAGING.currencies c
    WHERE c.date_update = '{{since}}'
    UNION
	SELECT t.currency_code
    FROM STV2024021912__STAGING.transactions t
    WHERE t.transaction_dt BETWEEN '{{since}}' AND '{{before}}'
) v
WHERE NOT EXISTS (
    SELECT 1
    FROM STV2024021912__DWH.dm_currencies c
    WHERE c.currency_code = v.currency_code
);
