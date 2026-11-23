-- Dim_Date: generated calendar 2020-01-01 .. 2030-12-31 (+ unknown member -1). Inserts only missing days.
INSERT INTO dim_date
SELECT -1, NULL, NULL, NULL, 'Unknown', NULL, NULL, NULL, 'Unknown', NULL, NULL, 'Unknown', NULL
WHERE NOT EXISTS (SELECT 1 FROM dim_date WHERE date_key = -1);

INSERT INTO dim_date
SELECT CAST(strftime(d, '%Y%m%d') AS INTEGER)          AS date_key,
       CAST(d AS DATE)                                  AS full_date,
       dayofmonth(d), isodow(d), dayname(d), isodow(d) IN (6, 7),
       weekofyear(d), month(d), monthname(d), quarter(d), year(d),
       strftime(d, '%Y-%m'), CAST(date_trunc('month', d) AS DATE)
FROM range(TIMESTAMP '2020-01-01', TIMESTAMP '2031-01-01', INTERVAL 1 DAY) t(d)
WHERE CAST(strftime(d, '%Y%m%d') AS INTEGER) NOT IN (SELECT date_key FROM dim_date);
