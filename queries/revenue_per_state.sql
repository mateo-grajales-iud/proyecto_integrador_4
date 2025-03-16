-- TODO: Esta consulta devolverá una tabla con dos columnas; customer_state y Revenue.
-- La primera contendrá las abreviaturas que identifican a los 10 estados con mayores ingresos,
-- y la segunda mostrará el ingreso total de cada uno.
-- PISTA: Todos los pedidos deben tener un estado "delivered" y la fecha real de entrega no debe ser nula.
select
	oc.customer_state as customer_state,
	sum(oop.payment_value) as Revenue
from
	olist_orders oo 
join
	olist_customers oc 
on
	oo.customer_id = oc.customer_id
join
	olist_order_payments oop
on
	oo.order_id = oop.order_id
where
	oo.order_status = "delivered"
	and oo.order_delivered_customer_date is not null
group by
	oc.customer_state
order BY
	Revenue desc
limit 10