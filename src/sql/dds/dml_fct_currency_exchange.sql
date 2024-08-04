INSERT INTO STV2024021912__DWH.fct_currency_exchange(
    id, 
    currency_id, 
    currency_id_with, 
    update_dt, 
    currency_with_div, 
    load_dt, 
    load_src
)
SELECT 
    HASH(dc.currency_id, dw.currency_id, c.update_dt) AS id, 
    dc.currency_id, 
    dw.currency_id AS currency_id_with, 
    c.update_dt, 
    c.currency_with_div, 
    now() AS load_dt, 
    'e-commerce-yapg' AS load_src
FROM (
    SELECT DISTINCT 
    c.currency_code, 
    c.currency_code_with, 
    c.date_update AS update_dt, 
    c.currency_with_div
    FROM STV2024021912__STAGING.currencies c
    WHERE c.date_update BETWEEN '{{since}}' AND '{{before}}'
) c
JOIN STV2024021912__DWH.dm_currencies dc
ON dc.currency_code = c.currency_code
JOIN STV2024021912__DWH.dm_currencies dw
ON dw.currency_code = c.currency_code_with
WHERE NOT EXISTS (
    SELECT 1
    FROM STV2024021912__DWH.fct_currency_exchange fce
    WHERE fce.id = HASH(dc.currency_id, dw.currency_id, c.update_dt)
);
