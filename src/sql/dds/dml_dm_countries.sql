INSERT INTO STV2024021912__DWH.dm_countries(country_id, country_name, load_dt, load_src)
SELECT 
    HASH(v.country_name) AS country_id, 
    v.country_name, 
    now() AS load_dt, 
    'e-commerce-yapg' AS load_src
FROM (
    SELECT DISTINCT t.country AS country_name
    FROM STV2024021912__STAGING.transactions t
    WHERE t.transaction_dt BETWEEN '{{since}}' AND '{{before}}'
) v
WHERE NOT EXISTS (
    SELECT 1
    FROM STV2024021912__DWH.dm_countries c
    WHERE c.country_name = v.country_name
);
