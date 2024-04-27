src_tables = [
    {"table": "address", "query": "SELECT * FROM src01.address;"},
    {"table": "customer", "query": "SELECT * FROM src01.customer;"},
    {"table": "channel", "query": "SELECT * FROM src01.channel;"},
    {"table": "order", "query": "SELECT * FROM src01.order;"},
    {"table": "order_detail", "query": "SELECT * FROM src01.order_detail;"},
    {"table": "payment", "query": "SELECT * FROM src01.payment;"},
    {"table": "payment_type", "query": "SELECT * FROM src01.payment_type;"},
    {"table": "product", "query": "SELECT * FROM src01.product;"},
]

data_platform_tables = ["customer", "product", "time"]