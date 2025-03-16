-- TODO: Esta consulta devolverá una tabla con las 10 categorías con mayores ingresos
-- (en inglés), el número de pedidos y sus ingresos totales. La primera columna será
-- Category, que contendrá las 10 categorías con mayores ingresos; la segunda será
-- Num_order, con el total de pedidos de cada categoría; y la última será Revenue,
-- con el ingreso total de cada categoría.
-- PISTA: Todos los pedidos deben tener un estado 'delivered' y tanto la categoría
-- como la fecha real de entrega no deben ser nulas.
select
	pcnt.product_category_name_english Category,
	count(DISTINCT oo.order_id) Num_order,
	sum((ooi.price + ooi.freight_value)) Revenue
from
	olist_orders oo 
join olist_order_items ooi on
	oo.order_id = ooi.order_id
join olist_products op on
	ooi.product_id = op.product_id
join product_category_name_translation pcnt on
	op.product_category_name = pcnt.product_category_name
where
	oo.order_status = "delivered"
	and op.product_category_name is not null
	and oo.order_delivered_customer_date is not null
group by
	pcnt.product_category_name_english
order by
	Revenue desc
limit 10