let nodes = [ {
    "id" : "orders$1",
    "name" : "ecommerce-data.orders",
    "type" : "import",
    "stage" : "streams",
    "inputs" : [ ]
}, {
    "id" : "orders$2",
    "name" : "Orders",
    "type" : "stream",
    "stage" : "streams",
    "inputs" : [ "orders$1" ],
    "plan" : "LogicalTableScan(table=[[orders$1]], hints=[[[WatermarkHint inheritPath:[] options:[1]]]])\n",
    "primary_key" : [ "_uuid" ],
    "timestamp" : "_ingest_time",
    "schema" : [ {
        "name" : "_uuid",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "_ingest_time",
        "type" : "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL"
    }, {
        "name" : "id",
        "type" : "BIGINT NOT NULL"
    }, {
        "name" : "customerid",
        "type" : "BIGINT NOT NULL"
    }, {
        "name" : "time",
        "type" : "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL"
    }, {
        "name" : "entries",
        "type" : "RecordType(INTEGER NOT NULL _idx, BIGINT NOT NULL productid, BIGINT NOT NULL quantity, DOUBLE NOT NULL unit_price, DOUBLE discount) NOT NULL ARRAY NOT NULL"
    } ],
    "post_processors" : [ ]
}, {
    "id" : "customer$1",
    "name" : "ecommerce-data.customer",
    "type" : "import",
    "stage" : "streams",
    "inputs" : [ ]
}, {
    "id" : "customer$2",
    "name" : "Customer",
    "type" : "stream",
    "stage" : "streams",
    "inputs" : [ "customer$1" ],
    "plan" : "LogicalTableScan(table=[[customer$1]], hints=[[[WatermarkHint inheritPath:[] options:[1]]]])\n",
    "primary_key" : [ "_uuid" ],
    "timestamp" : "_ingest_time",
    "schema" : [ {
        "name" : "_uuid",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "_ingest_time",
        "type" : "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL"
    }, {
        "name" : "customerid",
        "type" : "BIGINT NOT NULL"
    }, {
        "name" : "email",
        "type" : "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "name",
        "type" : "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "lastUpdated",
        "type" : "BIGINT NOT NULL"
    } ],
    "post_processors" : [ ]
}, {
    "id" : "product$1",
    "name" : "ecommerce-data.product",
    "type" : "import",
    "stage" : "streams",
    "inputs" : [ ]
}, {
    "id" : "product$2",
    "name" : "Product",
    "type" : "stream",
    "stage" : "streams",
    "inputs" : [ "product$1" ],
    "plan" : "LogicalTableScan(table=[[product$1]], hints=[[[WatermarkHint inheritPath:[] options:[1]]]])\n",
    "primary_key" : [ "_uuid" ],
    "timestamp" : "_ingest_time",
    "schema" : [ {
        "name" : "_uuid",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "_ingest_time",
        "type" : "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL"
    }, {
        "name" : "productid",
        "type" : "BIGINT NOT NULL"
    }, {
        "name" : "name",
        "type" : "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "description",
        "type" : "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "category",
        "type" : "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"
    } ],
    "post_processors" : [ ]
}, {
    "id" : "ordercustomer3$1",
    "name" : "OrderCustomer3",
    "type" : "stream",
    "stage" : "database",
    "inputs" : [ "orders$2", "customer$2", "product$2" ],
    "plan" : "LogicalProject(id=[$2], name=[$10], name0=[$15], _uuid2=[$0], _uuid00=[$6], _uuid10=[$12], __timestamp0=[CASE(<($1, $7), CASE(<($7, $13), $13, $7), CASE(<($1, $13), $13, $1))])\n  LogicalJoin(condition=[=($14, $8)], joinType=[inner])\n    LogicalJoin(condition=[=($3, $8)], joinType=[inner])\n      LogicalTableScan(table=[[orders$2]])\n      LogicalTableScan(table=[[customer$2]])\n    LogicalTableScan(table=[[product$2]])\n",
    "primary_key" : [ "_uuid2", "_uuid00", "_uuid10" ],
    "timestamp" : "__timestamp0",
    "schema" : [ {
        "name" : "id",
        "type" : "BIGINT NOT NULL"
    }, {
        "name" : "name",
        "type" : "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "name0",
        "type" : "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "_uuid2",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "_uuid00",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "_uuid10",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "__timestamp0",
        "type" : "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL"
    } ],
    "post_processors" : [ ]
}, {
    "id" : "ordercustomer2$1",
    "name" : "OrderCustomer2",
    "type" : "stream",
    "stage" : "database",
    "inputs" : [ "orders$2", "customer$2" ],
    "plan" : "LogicalProject(id=[$2], name=[$10], timestamp=[GREATEST($1, $7)], _uuid1=[$0], _uuid00=[$6])\n  LogicalJoin(condition=[=($3, $8)], joinType=[inner])\n    LogicalTableScan(table=[[orders$2]])\n    LogicalTableScan(table=[[customer$2]])\n",
    "primary_key" : [ "_uuid1", "_uuid00" ],
    "timestamp" : "timestamp",
    "schema" : [ {
        "name" : "id",
        "type" : "BIGINT NOT NULL"
    }, {
        "name" : "name",
        "type" : "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "timestamp",
        "type" : "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL"
    }, {
        "name" : "_uuid1",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "_uuid00",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    } ],
    "post_processors" : [ ]
}, {
    "id" : "ordercustomer1$1",
    "name" : "OrderCustomer1",
    "type" : "stream",
    "stage" : "database",
    "inputs" : [ "orders$2", "customer$2" ],
    "plan" : "LogicalProject(id=[$2], name=[$10], _uuid1=[$0], _uuid00=[$6], __timestamp0=[CASE(<($1, $7), $7, $1)])\n  LogicalJoin(condition=[=($3, $8)], joinType=[inner])\n    LogicalTableScan(table=[[orders$2]])\n    LogicalTableScan(table=[[customer$2]])\n",
    "primary_key" : [ "_uuid1", "_uuid00" ],
    "timestamp" : "__timestamp0",
    "schema" : [ {
        "name" : "id",
        "type" : "BIGINT NOT NULL"
    }, {
        "name" : "name",
        "type" : "VARCHAR(2147483647) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "_uuid1",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "_uuid00",
        "type" : "CHAR(36) CHARACTER SET \"UTF-16LE\" NOT NULL"
    }, {
        "name" : "__timestamp0",
        "type" : "TIMESTAMP_WITH_LOCAL_TIME_ZONE(3) NOT NULL"
    } ],
    "post_processors" : [ ]
}, {
    "id" : "ordercustomer3-query",
    "name" : "ordercustomer3-query",
    "type" : "query",
    "stage" : "database",
    "inputs" : [ "ordercustomer3$1" ],
    "plan" : "LogicalTableScan(table=[[ordercustomer3$1]])\n"
}, {
    "id" : "ordercustomer2-query",
    "name" : "ordercustomer2-query",
    "type" : "query",
    "stage" : "database",
    "inputs" : [ "ordercustomer2$1" ],
    "plan" : "LogicalTableScan(table=[[ordercustomer2$1]])\n"
}, {
    "id" : "ordercustomer1-query",
    "name" : "ordercustomer1-query",
    "type" : "query",
    "stage" : "database",
    "inputs" : [ "ordercustomer1$1" ],
    "plan" : "LogicalTableScan(table=[[ordercustomer1$1]])\n"
} ];