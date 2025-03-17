-- TODO: Esta consulta devolverá una tabla con las diferencias entre los tiempos 
-- reales y estimados de entrega por mes y año. Tendrá varias columnas: 
-- month_no, con los números de mes del 01 al 12; month, con las primeras 3 letras 
-- de cada mes (ej. Ene, Feb); Year2016_real_time, con el tiempo promedio de 
-- entrega real por mes de 2016 (NaN si no existe); Year2017_real_time, con el 
-- tiempo promedio de entrega real por mes de 2017 (NaN si no existe); 
-- Year2018_real_time, con el tiempo promedio de entrega real por mes de 2018 
-- (NaN si no existe); Year2016_estimated_time, con el tiempo promedio estimado 
-- de entrega por mes de 2016 (NaN si no existe); Year2017_estimated_time, con 
-- el tiempo promedio estimado de entrega por mes de 2017 (NaN si no existe); y 
-- Year2018_estimated_time, con el tiempo promedio estimado de entrega por mes 
-- de 2018 (NaN si no existe).
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
-- 3. Considera tomar order_id distintos.
with delivery_times as (
select
		julianday(oo.order_delivered_customer_date) - julianday(oo.order_purchase_timestamp) as real_time,
		julianday(oo.order_estimated_delivery_date) - julianday(oo.order_purchase_timestamp) as estimated_time,
		STRFTIME("%m", oo.order_purchase_timestamp) as month_no,
		case strftime("%m", oo.order_purchase_timestamp)
			WHEN '01' THEN 'Ene'
			WHEN '02' THEN 'Feb'
			WHEN '03' THEN 'Mar'
			WHEN '04' THEN 'Abr'
			WHEN '05' THEN 'May'
			WHEN '06' THEN 'Jun'
			WHEN '07' THEN 'Jul'
			WHEN '08' THEN 'Ago'
			WHEN '09' THEN 'Sep'
			WHEN '10' THEN 'Oct'
			WHEN '11' THEN 'Nov'
			WHEN '12' THEN 'Dic'
		end as month,
	    strftime("%Y", oo.order_purchase_timestamp) as year_date
from
	olist_orders oo
where
	oo.order_status = "delivered"
	and oo.order_delivered_customer_date is not null
)
select
	d.month_no,
	d.month,
	avg(case when d.year_date = "2016" then d.real_time end) as Year2016_real_time,
	avg(case when d.year_date = "2017" then d.real_time end) as Year2017_real_time,
	avg(case when d.year_date = "2018" then d.real_time end) as Year2018_real_time,
	avg(case when d.year_date = "2016" then d.estimated_time end) as Year2016_estimated_time,
	avg(case when d.year_date = "2017" then d.estimated_time end) as Year2017_estimated_time,
	avg(case when d.year_date = "2018" then d.estimated_time end) as Year2018_estimated_time
from
	delivery_times d
group by
	d.month_no,
	d.month
having
	d.month_no is not null
order by
	d.month_no