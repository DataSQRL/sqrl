//package com.datasqrl.testbed;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.ResultSet;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.concurrent.TimeUnit;
//import lombok.SneakyThrows;
//import org.apache.flink.api.common.typeinfo.Types;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.TableResult;
//import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import java.util.Arrays;
//import java.util.List;
//import org.testcontainers.containers.PostgreSQLContainer;
//import org.testcontainers.junit.jupiter.Container;
//import org.testcontainers.utility.DockerImageName;
//
//public class FlinkTestBed {
//  @Container
//  static PostgreSQLContainer testDatabase = new PostgreSQLContainer(
//      DockerImageName.parse("ankane/pgvector:v0.5.0")
//          .asCompatibleSubstituteFor("postgres"));
//
//  private static Connection getPostgresConnection() throws Exception {
//    return DriverManager.getConnection(
//        testDatabase.getJdbcUrl(),
//        testDatabase.getUsername(),
//        testDatabase.getPassword()
//    );
//  }
//
//  @SneakyThrows
//  private static void createPostgresTable() {
//    try (Connection conn = getPostgresConnection();
//        Statement stmt = conn.createStatement()) {
//      String createTableSQL = "CREATE TABLE IF NOT EXISTS jsondata ("
//          + "x text PRIMARY KEY);";
//      stmt.execute(createTableSQL);
//    }
//  }
//
//  public static void main(String[] args) throws Exception {
//    testDatabase.start();
//    // Set up the streaming execution environment
//    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//    env.setParallelism(1);
//    createPostgresTable();
//    // Create a Table Environment
//    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//
//    // Input data
//    List<Row> inputRows = Arrays.asList(
//        Row.of(1, "{\"name\":\"John\", \"age\":30, \"cars\":[\"Ford\", \"BMW\", \"Fiat\"]}"),
//        Row.of(2, "{\"name\":\"Jane\", \"age\":25, \"cars\":[\"Tesla\", \"Audi\"]}")
//    );
//
//    // Create a Table from the list of rows
//    Table inputTable = tableEnv.fromDataStream(
//        env.fromCollection(inputRows,
//            Types.ROW_NAMED(new String[]{"id", "json"},
//                Types.INT, Types.STRING))
//    );
//
//    // Register the Table under a name
//    tableEnv.createTemporaryView("people", inputTable);
//
//    // Define a SQL query to parse JSON and extract specific fields
//    String query = "SELECT JSON_ARRAYAGG(id NULL ON NULL) AS x FROM people";
//
//    // Execute the query
//    Table result = tableEnv.sqlQuery(query);
//
//    Table table = tableEnv.sqlQuery("SELECT x FROM " + result);
//
//    String sinkDDL = "CREATE TABLE mySinkTable ( " +
//        "   x VARCHAR(255)," +
//        "   PRIMARY KEY (x) NOT ENFORCED " +
//        ") WITH (" +
//        "   'connector' = 'jdbc', " +
//        "   'url' = '"+testDatabase.getJdbcUrl()+"', " +
//        "   'table-name' = 'jsondata', " +
//        "   'username' = '"+ testDatabase.getUsername()+"', " +
//        "   'password' = '"+testDatabase.getPassword()+"'" +
//        ")";
//
//    tableEnv.executeSql(sinkDDL);
//
//    // Insert into the Sink Table
//    tableEnv.executeSql("INSERT INTO mySinkTable SELECT x AS x3 FROM " + result);
//
//
//    table.execute().print();
//
//    Object o = executePostgresQuery("SELECT * FROM jsondata");
//    System.out.println(o);
//
////
//////    TableResult execute = table.execute();
//////    execute.await(10, TimeUnit.SECONDS);
////    DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(table, Row.class);
////
////    // Print the result to the console (for demonstration purposes)
////    rowDataStream.print();
////
////    rowDataStream.executeAndCollect();
////    List<Row> rows = new ArrayList<>();
////    execute.collect().forEachRemaining(rows::add);
//
////    System.out.println(rows);
////
////
////        DataStream<Tuple2<Boolean, Row>> rowDataStream = tableEnv.toRetractStream(result, Row.class);
////
////        // Print the result to the console (for demonstration purposes)
////        rowDataStream.print();
//
//    // Execute the Flink job
//  }
//
//
//  @SneakyThrows
//  private static Object executePostgresQuery(String query) {
//    try (Connection conn = getPostgresConnection();
//        Statement stmt = conn.createStatement()) {
//      ResultSet rs = stmt.executeQuery(query);
//      // Assuming the result is a single value for simplicity
//      return rs.next() ? rs.getObject(1) : null;
//    }
//  }
//}
