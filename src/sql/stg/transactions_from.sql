SELECT * FROM transactions 
WHERE transaction_dt BETWEEN '{{since}}' AND ('{{before}}')