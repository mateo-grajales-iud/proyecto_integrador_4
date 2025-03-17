-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).
-- Se necesita tener las gemas del infinito o las bolas del dragon para adivinar
-- que se necesita hacer con estas instrucciones
WITH stage AS (
    SELECT
        oo.order_id,
        oo.customer_id,
        oo.order_delivered_customer_date,
        oop.payment_value
    FROM olist_orders oo
    JOIN olist_order_payments oop 
        ON oo.order_id = oop.order_id
    WHERE oo.order_delivered_customer_date IS NOT NULL
        AND oo.order_status = "delivered"
    GROUP BY oo.order_id  --Este GROUP BY puede generar problemas si hay múltiples pagos por pedido
    ORDER BY oo.order_delivered_customer_date
)
SELECT
    strftime('%m', s.order_delivered_customer_date) AS month_no,
    case strftime("%m", s.order_delivered_customer_date)
			WHEN '01' THEN 'Jan'
			WHEN '02' THEN 'Feb'
			WHEN '03' THEN 'Mar'
			WHEN '04' THEN 'Apr'
			WHEN '05' THEN 'May'
			WHEN '06' THEN 'Jun'
			WHEN '07' THEN 'Jul'
			WHEN '08' THEN 'Aug'
			WHEN '09' THEN 'Sep'
			WHEN '10' THEN 'Oct'
			WHEN '11' THEN 'Nov'
			WHEN '12' THEN 'Dec'
		end as month,
    SUM(CASE WHEN strftime('%Y', s.order_delivered_customer_date) = '2016' THEN s.payment_value ELSE 0.00 END) AS Year2016,
    SUM(CASE WHEN strftime('%Y', s.order_delivered_customer_date) = '2017' THEN s.payment_value ELSE 0.00 END) AS Year2017,
    SUM(CASE WHEN strftime('%Y', s.order_delivered_customer_date) = '2018' THEN s.payment_value ELSE 0.00 END) AS Year2018
FROM stage s
GROUP BY month_no, month
ORDER BY month_no