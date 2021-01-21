select
    stg_orders.order_id,
    stg_orders.customer_id,
    stg_orders.order_date,
    stg_orders.status,
    SUM(amount_dollars) as all_amount_usd

from {{ref('stg_orders')}} stg_orders

left join {{ref('stg_payments')}} stg_payments
on stg_orders.order_id = stg_payments.order_id

where stg_payments.payment_status = 'success'

group by 
    stg_orders.order_id,
    stg_orders.customer_id,
    stg_orders.order_date,
    stg_orders.status