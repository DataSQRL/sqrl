package com.datasqrl;

import java.util.Map;
import lombok.SneakyThrows;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;


public class FlinkTest {

  public static void main(String[] args) {
    FlinkTest flinkTest = new FlinkTest();
    flinkTest.run();
  }

  @SneakyThrows
  private void run() {
    Map<String, String> config = Map.of(
        "taskmanager.network.memory.max", "1g",
        "table.exec.source.idle-timeout", "1 s");
    //read flink config from package.json values?

    Configuration configuration = Configuration.fromMap(config);
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.createLocalEnvironment(configuration);
    sEnv.setParallelism(1);
    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(configuration).build();
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
    TableResult tableResult = null;

    // Step 2: Execute the SQL statements
    tableEnv.executeSql("CREATE CATALOG my_catalog WITH (\n" +
        "'type' = 'iceberg',\n" +
        "'catalog-type' = 'hadoop',\n" +
        "'warehouse' = 'file:///Users/henneberger/sqrl/sqrl-testing/sqrl-flink-1.18/src/test'\n" +
        ");");

    tableEnv.executeSql("USE CATALOG my_catalog;");

    tableEnv.executeSql("CREATE TABLE my_table (\n" +
        "  id BIGINT,\n" +
        "  name STRING,\n" +
        "  ts TIMESTAMP\n" +
        ") PARTITIONED BY (ts)\n" +
        "WITH (\n" +
        "'format-version' = '2',\n" +
        "'write-format' = 'parquet',\n" +
        "'write.target-file-size-bytes' = '134217728',\n" +
        "'write.upsert.enabled' = 'false'\n" +
        ");");

    tableEnv.executeSql("INSERT INTO my_table VALUES\n" +
        "  (1, 'Alice', TIMESTAMP '2024-08-16 12:00:00'),\n" +
        "  (2, 'Bob', TIMESTAMP '2024-08-16 13:00:00'),\n" +
        "  (3, 'Charlie', TIMESTAMP '2024-08-16 14:00:00');");

    Thread.sleep(1000);
    // Step 3: Query the data and print the results
    TableResult result = tableEnv.executeSql("SELECT * FROM my_table;");

    // Print the results to the console
    result.print();
  }
}
