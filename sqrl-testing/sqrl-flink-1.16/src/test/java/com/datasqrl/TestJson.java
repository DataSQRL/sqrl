package com.datasqrl;

import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.Test;

public class TestJson {


  @Test
  public void test() {

    Configuration sEnvConfig = Configuration.fromMap(Map.of());
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(
        sEnvConfig);
    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(Map.of())).build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);

    tEnv.executeSql("CREATE TEMPORARY FUNCTION IF NOT EXISTS `jsonobject` AS 'com.datasqrl.json.JsonObject' LANGUAGE JAVA;\n");
    tEnv.executeSql("CREATE TEMPORARY FUNCTION IF NOT EXISTS `jsonobjectagg` AS 'com.datasqrl.json.JsonObjectAgg' LANGUAGE JAVA;\n");
    tEnv.executeSql("CREATE TEMPORARY FUNCTION IF NOT EXISTS `jsonArrayAgg` AS 'com.datasqrl.json.JsonArrayAgg' LANGUAGE JAVA;\n");
    tEnv.executeSql("CREATE TEMPORARY FUNCTION IF NOT EXISTS `jsontostring` AS 'com.datasqrl.json.JsonToString' LANGUAGE JAVA;\n");
    TableResult tableResult = tEnv.executeSql("CREATE TABLE Orders (\n"
        + "    order_number BIGINT,\n"
        + "    price        DECIMAL(32,2),\n"
        + "    buyer        ROW<first_name STRING, last_name STRING>,\n"
        + "    order_time   TIMESTAMP(3)\n"
        + ") WITH (\n"
        + "  'connector' = 'datagen',"
        + "  'number-of-rows' = '100'\n"
        + ")");

    TableResult tableResult2 = tEnv.executeSql("SELECT jsonToString(jsonArrayAgg(\n"
        + "        jsonObject(\n"
        + "            'key', order_number"
        + "        )))  FROM Orders;");
    tableResult2.print();
  }
}
