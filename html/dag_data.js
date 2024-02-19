let nodes = [
    {
        "id": "orders$1",
        "name": "datasqrl.example.nutshop.Orders",
        "type": "import",
        "stage": "stream",
        "inputs": []
    },
    {
        "id": "orders$2",
        "name": "Orders",
        "type": "stream",
        "primary_key": ["uuid"],
        "schema": [
            {
                "name": "uuid",
                "type": "STRING",
                "qualifier": "NOT NULL"
            },
            {
                "name": "orderid",
                "type": "INT"
            },
            {
                "name": "time",
                "type": "TIMESTAMP",
                "qualifier": "NOT NULL"
            }
        ],
        "timestamp": "time",
        "plan": "full relnode",
        "stage": "stream",
        "inputs": ["orders$1"]
    },
    {
        "id": "orders$3",
        "name": "Orders",
        "type": "state",
        "primary_key": ["orderid"],
        "schema": [
            {
                "name": "uuid",
                "type": "STRING",
                "qualifier": "NOT NULL"
            },
            {
                "name": "orderid",
                "type": "INT"
            },
            {
                "name": "time",
                "type": "TIMESTAMP",
                "qualifier": "NOT NULL"
            }
        ],
        "timestamp": "time",
        "plan": "full relnode",
        "stage": "stream",
        "post_processors": [
            {
                "name": "dedup",
                "keys": ["orderid"]
            }
        ],
        "inputs": ["orders$2"]
    },
    {
        "id": "orders$3$queries",
        "name": "2",
        "type": "query",
        "stage": "database",
        "inputs": ["orders$3"]
    },
    {
        "id": "orders_total$1",
        "name": "Orders.total",
        "type": "state",
        "primary_key": ["_pk"],
        "schema": [
            {
                "name": "_pk",
                "type": "INT",
                "qualifier": "NOT NULL"
            },
            {
                "name": "total",
                "type": "INT"
            },
            {
                "name": "_timestamp",
                "type": "TIMESTAMP",
                "qualifier": "NOT NULL"
            }
        ],
        "timestamp": "_timestamp",
        "plan": "full relnode",
        "stage": "database",
        "inputs": ["orders$2", "orders$3"]
    },
    {
        "id": "orders_total$1$queries",
        "name": "1",
        "type": "query",
        "stage": "database",
        "inputs": ["orders_total$1"]
    },
    {
        "id": "orders_total$3$export",
        "name": "output.orders",
        "type": "export",
        "stage": "stream",
        "inputs": ["orders$3"]
    }

];
