-- TODO: Esta consulta devolverá una tabla con dos columnas: Estado y 
-- Diferencia_Entrega. La primera contendrá las letras que identifican los 
-- estados, y la segunda mostrará la diferencia promedio entre la fecha estimada 
-- de entrega y la fecha en la que los productos fueron realmente entregados al 
-- cliente.
-- PISTAS:
-- 1. Puedes usar la función julianday para convertir una fecha a un número.
-- 2. Puedes usar la función CAST para convertir un número a un entero.
-- 3. Puedes usar la función STRFTIME para convertir order_delivered_customer_date a una cadena, eliminando horas, minutos y segundos.
-- 4. order_status == 'delivered' AND order_delivered_customer_date IS NOT NULL
select 
	oc.customer_state as State,
	CAST(AVG(julianday(strftime('%Y-%m-%d', order_estimated_delivery_date)) - 
         julianday(strftime('%Y-%m-%d', order_delivered_customer_date))) 
     AS INTEGER) as Delivery_Difference
from olist_orders oo 
join olist_customers oc on
	oo.customer_id = oc.customer_id
where
	oo.order_status == "delivered"
	and oo.order_delivered_customer_date is not null
group by
	oc.customer_state 
order by
	Delivery_Difference