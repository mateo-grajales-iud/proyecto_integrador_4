-- TODO: Esta consulta devolverá una tabla con dos columnas: estado_pedido y
-- Cantidad. La primera contendrá las diferentes clases de estado de los pedidos,
-- y la segunda mostrará el total de cada uno.
select
	oo.order_status,
	count(oo.order_status) as Ammount
from olist_orders oo 
group by
	oo.order_status