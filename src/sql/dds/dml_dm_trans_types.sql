INSERT INTO STV2024021912__DWH.dm_trans_types(trans_type_id, trans_type, load_dt, load_src)
SELECT 
    HASH(v.trans_type) AS trans_type_id, 
    v.trans_type, 
    now() AS load_dt, 
    'e-commerce-yapg' AS load_src
  FROM (
    SELECT DISTINCT t.transaction_type AS trans_type
    FROM STV2024021912__STAGING.transactions t
    WHERE t.transaction_dt BETWEEN '{{since}}' AND '{{before}}'
) v
WHERE NOT EXISTS (
    SELECT 1
    FROM STV2024021912__DWH.dm_trans_types tr
    WHERE tr.trans_type = v.trans_type
);
