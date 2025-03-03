-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).
SELECT 
    strftime('%m', o.order_purchase_timestamp) AS month_no,
    substr('JanFebMarAprMayJunJulAugSepOctNovDec', (strftime('%m', o.order_purchase_timestamp) - 1) * 3 + 1, 3) AS month,
    COALESCE(SUM(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2016' THEN i.price END), 0.00) AS Year2016,
    COALESCE(SUM(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2017' THEN i.price END), 0.00) AS Year2017,
    COALESCE(SUM(CASE WHEN strftime('%Y', o.order_purchase_timestamp) = '2018' THEN i.price END), 0.00) AS Year2018
FROM olist_orders o
JOIN olist_order_items i ON o.order_id = i.order_id
GROUP BY month_no
ORDER BY month_no;
