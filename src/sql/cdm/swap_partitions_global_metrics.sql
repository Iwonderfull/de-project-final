SELECT swap_partitions_between_tables (
    'STV2024021912__DWH.global_metrics_copy', 
    '{{since}}',
    '{{since}}',
    'STV2024021912__DWH.global_metrics'
);
