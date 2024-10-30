package com.datasqrl;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.types.Row;
import org.junit.Test;

public class FlinkTableApiTest {

    @Test
    public void testCustomerOrdersView() throws Exception {
        // Set up the environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // Define the Orders table
        tableEnv.executeSql(
            "CREATE TABLE orders (" +
            "    id String," +
            "    c_id INT," +
            "    p_id INT," +
            "    order_time TIMESTAMP(3)" +
            ") WITH (" +
            "    'connector' = 'datagen'," +
            "    'rows-per-second' = '1'," +
            "    'fields.id.kind' = 'sequence'," +
            "    'fields.id.start' = '1'," +
            "    'fields.id.end' = '100'," +
            "    'fields.c_id.min' = '1'," +
            "    'fields.c_id.max' = '20'," +
            "    'fields.p_id.min' = '1'," +
            "    'fields.p_id.max' = '50'" +
            ")"
        );

        // Define the Products table with deduplication
        tableEnv.executeSql(
            "CREATE TABLE products (" +
            "    id INT," +
            "    name STRING," +
            "    description STRING," +
            "    price DECIMAL(10, 2)" +
            ") WITH (" +
            "    'connector' = 'datagen'," +
            "    'rows-per-second' = '1'," +
            "    'fields.id.kind' = 'sequence'," +
            "    'fields.id.start' = '1'," +
            "    'fields.id.end' = '50'" +
            ")"
        );

        Table productsTable = tableEnv.from("products")
            .distinct()
            .as("id", "name", "description", "price");
        tableEnv.createTemporaryView("deduplicated_products", productsTable);

        // Define the Customers table with deduplication
        tableEnv.executeSql(
            "CREATE TABLE customers (" +
            "    c_id INT," +
            "    name STRING," +
            "    email STRING" +
            ") WITH (" +
            "    'connector' = 'datagen'," +
            "    'rows-per-second' = '1'," +
            "    'fields.c_id.kind' = 'sequence'," +
            "    'fields.c_id.start' = '1'," +
            "    'fields.c_id.end' = '20'" +
            ")"
        );

        Table customersTable = tableEnv.from("customers")
            .distinct()
            .as("c_id", "name", "email");
        tableEnv.createTemporaryView("deduplicated_customers", customersTable);

        // Create the CustomerOrders view
        tableEnv.executeSql(
            "CREATE VIEW CustomerOrders AS " +
            "SELECT * " +
            "FROM orders " +
            "INNER JOIN deduplicated_customers ON orders.c_id = deduplicated_customers.c_id " +
            "INNER JOIN deduplicated_products ON orders.p_id = deduplicated_products.id"
        );

        // Execute a query on the view
        Table resultTable = tableEnv.sqlQuery("SELECT * FROM CustomerOrders");
        tableEnv.toChangelogStream(resultTable)
            .print();

        // Execute the environment
        env.execute();
    }
}
