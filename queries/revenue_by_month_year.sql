-- TODO: Esta consulta devolverá una tabla con los ingresos por mes y año.
-- Tendrá varias columnas: month_no, con los números de mes del 01 al 12;
-- month, con las primeras 3 letras de cada mes (ej. Ene, Feb);
-- Year2016, con los ingresos por mes de 2016 (0.00 si no existe);
-- Year2017, con los ingresos por mes de 2017 (0.00 si no existe); y
-- Year2018, con los ingresos por mes de 2018 (0.00 si no existe).
-- Se necesita tener las gemas del infinito o las bolas del dragon para adivinar
-- que se necesita hacer con estas instrucciones
select
	strftime('%m', oo.order_delivered_customer_date) AS month_no,
    substr('JanFebMarAprMayJunJulAugSepOctNovDec', (strftime('%m', oo.order_delivered_carrier_date) - 1) * 3 + 1, 3) AS month,
    COALESCE(SUM(CASE WHEN strftime('%Y', oo.order_delivered_carrier_date) = '2016' THEN oop.payment_value END), 0.00) AS Year2016,
    COALESCE(SUM(CASE WHEN strftime('%Y', oo.order_delivered_carrier_date) = '2017' THEN oop.payment_value END), 0.00) AS Year2017,
    COALESCE(SUM(CASE WHEN strftime('%Y', oo.order_delivered_carrier_date) = '2018' THEN oop.payment_value END), 0.00) AS Year2018
from
	olist_orders oo
join
	olist_order_payments oop 
on
	oo.order_id = oop.order_id
where 
	oo.order_delivered_customer_date is not null
	and oo.order_status = "delivered"
group by
	month_no
order by
	month_no