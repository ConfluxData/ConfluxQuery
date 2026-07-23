SELECT
    CAST(1 AS BIGINT) AS id,
    CAST(123.456 AS DECIMAL(10, 3)) AS amount,
    'qcli-unicode-✓' AS text_value,
    CAST(NULL AS VARCHAR(100)) AS null_value
