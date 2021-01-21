select
    id as payment_id,
    orderid as order_id,
    paymentmethod as payment_method,
    status as payment_status,
    AMOUNT/100 as amount_dollars,
    created
from {{ source('stripe', 'payment') }}
