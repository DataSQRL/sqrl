package com.datasqrl;

import static com.datasqrl.function.SqrlFunction.getFunctionNameFromClass;
import static com.datasqrl.plan.local.analyze.RetailSqrlModule.createTableSource;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SourceFactory;
import com.datasqrl.engine.database.relational.ddl.PostgresDDLFactory;
import com.datasqrl.engine.database.relational.ddl.statements.CreateTableDDL;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.SqrlFunction;
import com.datasqrl.functions.json.StdJsonLibraryImpl;
import com.datasqrl.graphql.AbstractGraphqlTest;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.InMemSourceFactory;
import com.datasqrl.io.mem.MemoryConnectorFactory;
import com.datasqrl.json.FlinkJsonType;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.plan.global.PhysicalDAGPlan.EngineSink;
import com.datasqrl.plan.local.analyze.MockModuleLoader;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.TableConverter;
import com.datasqrl.plan.table.TableIdFactory;
import com.datasqrl.plan.validate.ScriptPlanner;
import com.datasqrl.util.SnapshotTest;
import com.google.auto.service.AutoService;
import com.ibm.icu.impl.Pair;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.ScriptNode;
import org.apache.calcite.sql.SqrlStatement;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.postgresql.util.PGobject;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

/**
 * A test suite to convert SQRL queries to their respective dialects
 */
@Slf4j
@ExtendWith(MiniClusterExtension.class)
public class JsonConversionTest extends AbstractGraphqlTest {

  protected SnapshotTest.Snapshot snapshot;
  ObjectMapper objectMapper = new ObjectMapper();
  private ScriptPlanner planner;

  @BeforeAll
  public static void setupAll() {
    createPostgresTable();
    insertDataIntoPostgresTable();
  }

  @SneakyThrows
  private static void createPostgresTable() {
    try (Connection conn = getPostgresConnection(); Statement stmt = conn.createStatement()) {
      String createTableSQL =
          "CREATE TABLE IF NOT EXISTS jsondata$2 (" + "id INT, " + "json jsonb);";
      stmt.execute(createTableSQL);
    }
  }

  @SneakyThrows
  private static void insertDataIntoPostgresTable() {
    try (Connection conn = getPostgresConnection(); Statement stmt = conn.createStatement()) {
      String insertSQL = "INSERT INTO jsondata$2 (id, json) VALUES "
          + "(1, '{\"example\":[1,2,3]}'),(2, '{\"example\":[4,5,6]}');";
      stmt.execute(insertSQL);
    }
  }
//
//  @AfterAll
//  public static void tearDownAll() {
////    testDatabase.stop();
//  }

  @BeforeEach
  public void setup(TestInfo testInfo) throws IOException {
    initialize(IntegrationTestSettings.getInMemory(), null, Optional.empty(), ErrorCollector.root(),
        createJson(), false);

    this.snapshot = SnapshotTest.Snapshot.of(getClass(), testInfo);

    this.planner = injector.getInstance(ScriptPlanner.class);
    runStatement("IMPORT json-data.jsondata");
  }

  private void runStatement(String statement) {
    planner.validateStatement(parse(statement));
  }

  private SqrlStatement parse(String statement) {
    return (SqrlStatement) ((ScriptNode)framework.getQueryPlanner().parse(Dialect.SQRL, statement))
        .getStatements().get(0);
  }

  public Map<NamePath, SqrlModule> createJson() {
    CalciteTableFactory tableFactory = new CalciteTableFactory(new TableIdFactory(new HashMap<>()),
        new TableConverter(new TypeFactory(), framework));
    SqrlModule module = new SqrlModule() {

      private final Map<Name, NamespaceObject> tables = new HashMap();

      @Override
      public Optional<NamespaceObject> getNamespaceObject(Name name) {
        NamespaceObject obj = new TableSourceNamespaceObject(
            createTableSource(JsonData.class, "data", "json-data"), tableFactory);
        return Optional.of(obj);
      }

      @Override
      public List<NamespaceObject> getNamespaceObjects() {
        return new ArrayList<>(tables.values());
      }
    };

    return Map.of(NamePath.of("json-data"), module);
  }

  @SneakyThrows
  private Object executePostgresQuery(String query) {
    try (Connection conn = getPostgresConnection(); Statement stmt = conn.createStatement()) {
      System.out.println(query);
      ResultSet rs = stmt.executeQuery(query);
      // Assuming the result is a single value for simplicity
      return rs.next() ? rs.getObject(1) : null;
    }
  }

  @SneakyThrows
  public Object jsonFunctionTest(String query) {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    // Assuming you have a method to create or get Flink SQL environment
    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

    List<Row> inputRows = Arrays.asList(Row.of(1, "{\"example\":[1,2,3]}"),
        Row.of(2, "{\"example\":[4,5,6]}"));

    // Create a Table from the list of rows
    Table inputTable = tableEnv.fromDataStream(env.fromCollection(inputRows,
        Types.ROW_NAMED(new String[]{"id", "json"}, Types.INT, Types.STRING)));

    for (FunctionDefinition sqrlFunction : StdJsonLibraryImpl.json) {
      UserDefinedFunction userDefinedFunction = (UserDefinedFunction) sqrlFunction;
      tableEnv.createFunction(getFunctionNameFromClass(sqrlFunction.getClass()),
          userDefinedFunction.getClass());
    }

    // Register the Table under a name
    tableEnv.createTemporaryView("jsondata$2", inputTable);

    // Run your query
    Table result = tableEnv.sqlQuery(query);
    TableResult execute = result.execute();
    List<Row> rows = new ArrayList<>();
    execute.collect().forEachRemaining(rows::add);

    return rows.get(rows.size() - 1).getField(0);
  }

  @AfterEach
  public void tearDown() {
    snapshot.createOrValidate();
  }

  @Test
  public void jsonArrayTest() {
    testJsonReturn("jsonArray('a', null, 'b', 123)");
  }

  @Test
  public void jsonArrayAgg() {
    testJsonReturn("jsonArrayAgg(jsonExtract(toJson(json), '$.example[0]', 0))");
  }

  @Test
  public void jsonObjectAgg() {
    testJsonReturn("jsonObjectAgg('key', toJson(json))");
  }

  @Test
  public void jsonArrayAgg2() {
    testJsonReturn("jsonArrayAgg(id)");
  }

  @Test
  public void jsonArrayAggNull() {
    testJsonReturn("jsonArrayAgg(toJson(null))");
  }

  @Test
  public void jsonArrayArray() {
    testJsonReturn("JSONARRAY(JSONARRAY(1))");
  }

  @Test
  public void jsonExistsTest() {
    testScalarReturn("jsonExists(toJson('{\"a\": true}'), '$.a')");
  }

  @Test
  public void jsonExtractTest() {
    testScalarReturn("jsonExtract(toJson('{\"a\": \"hello\"}'), '$.a', 'default')");
  }

  @Test
  public void jsonConcat() {
    testJsonReturn("jsonConcat(toJson('{\"a\": \"hello\"}'), toJson('{\"b\": \"hello\"}'))");
  }

  @Test
  public void jsonObjectTest() {
    testJsonReturn("jsonObject('key1', 'value1', 'key2', 123)");
  }

  @Test
  public void jsonQueryTest() {
    testJsonReturn("jsonQuery(toJson('{\"a\": {\"b\": 1}}'), '$.a')");
  }

  @Test
  public void jsonArrayWithNulls() {
    // Testing JSON array creation with null values
    testJsonReturn("jsonArray('a', null, 'b', null)");
  }

  @Test
  public void jsonObjectWithNulls() {
    // Testing JSON object creation with null values
    testJsonReturn("jsonObject('key1', null, 'key2', 'value2')");
  }

  @Test
  public void jsonExtract() {
    testScalarReturn("jsonExtract(toJson('{\"a\": \"hello\"}'), '$.a')");
  }

  @Test
  public void jsonExtractWithDefaultString() {
    // Test with a default string value
    testScalarReturn("jsonExtract(toJson('{\"a\": \"hello\"}'), '$.b', 'defaultString')");
  }

  @Test
  public void jsonExtractWithDefaultInteger() {
    // Test with a default integer value
    testScalarReturn("jsonExtract(toJson('{\"a\": \"hello\"}'), '$.b', 123)");
  }

  @Test
  public void jsonExtractWithDefaultBoolean() {
    // Test with a default boolean value
    testScalarReturn("jsonExtract(toJson('{\"a\": \"hello\"}'), '$.b', true)");
  }

  @Test
  public void jsonExtractWithDefaultBoolean2() {
    // Test with a default boolean value
    testScalarReturn("jsonExtract(toJson('{\"a\": false}'), '$.a', true)");
  }

  @Test
  public void jsonExtractWithDefaultDouble3() {
    // Test with a default boolean value
    testScalarReturn("jsonExtract(toJson('{\"a\": 0.2}'), '$.a', 0.0)");
  }

  @Test
  public void jsonExtractWithDefaultDouble4() {
    // Test with a default boolean value
    testScalarReturn("jsonExtract(toJson('{\"a\": 0.2}'), '$.a', 0)");
  }

  @Test
  public void jsonExtractWithDefaultNull() {
    // Test with a default null value
    testScalarReturn("jsonExtract(toJson('{\"a\": \"hello\"}'), '$.b', null)");
  }

  @Test
  public void jsonExtractWithNonexistentPath() {
    // Test extraction from a nonexistent path (should return default value)
    testScalarReturn("jsonExtract(toJson('{\"a\": \"hello\"}'), '$.nonexistent', 'default')");
  }

  @Test
  public void jsonExtractWithEmptyJson() {
    // Test extraction from an empty JSON object
    testScalarReturn("jsonExtract(toJson('{}'), '$.a', 'default')");
  }

  @Test
  public void jsonExtractWithComplexJsonPath() {
    // Test extraction with a complex JSON path
    testScalarReturn(
        "jsonExtract(toJson('{\"a\": {\"b\": {\"c\": \"value\"}}}'), '$.a.b.c', 'default')");
  }

  @Test
  public void jsonExtractWithArrayPath() {
    // Test extraction where the path leads to an array
    testScalarReturn("jsonExtract(toJson('{\"a\": [1, 2, 3]}'), '$.a[1]', 'default')");
  }

  @Test
  public void jsonExtractWithNumericDefault() {
    // Test extraction with a numeric default value
    testScalarReturn("jsonExtract(toJson('{\"a\": \"hello\"}'), '$.b', 0)");
  }

  @Test
  public void jsonObject() {
    // Test extraction with a numeric default value
    testJsonReturn("jsonObject('key', toJson('{\"a\": \"hello\"}'), 'key2', 0)");
  }

  @Test
  public void jsonArrayWithMixedDataTypes() {
    // Testing JSON array creation with mixed data types
    testJsonReturn("jsonArray('a', 1, true, null, 3.14)");
  }

  @Test
  public void jsonArrayWithNestedArrays() {
    // Testing JSON array creation with nested arrays
    testJsonReturn("jsonArray('a', jsonArray('nested', 1), 'b', jsonArray('nested', 2))");
  }

  @Test
  public void jsonArrayWithEmptyValues() {
    // Testing JSON array creation with empty values
    testJsonReturn("jsonArray('', '', '', '')");
  }

  @Test
  public void jsonObjectWithMixedDataTypes() {
    // Testing JSON object creation with mixed data types
    testJsonReturn("jsonObject('string', 'text', 'number', 123, 'boolean', true)");
  }

  @Test
  public void jsonObjectWithNestedObjects() {
    // Testing JSON object creation with nested objects
    testJsonReturn("jsonObject('key1', jsonObject('nestedKey', 'nestedValue'), 'key2', 'value2')");
  }

  @Test
  public void jsonObjectWithEmptyKeys() {
    // Testing JSON object creation with empty keys
    testJsonReturn("jsonObject('', 'value1', '', 'value2')");
  }

  @SneakyThrows
  private void testJsonReturn(String function) {
    Pair<Object, Object> x = executeScript(function);
    assertEquals(objectMapper.readTree((String) x.first), objectMapper.readTree((String) x.second));
  }

  @SneakyThrows
  private void testScalarReturn(String function) {
    Pair<Object, Object> x = executeScript(function);
    assertEquals(x.first.toString().trim(), x.second.toString().trim());
  }

  public Pair<Object, Object> executeScript(String fncName) {
    runStatement("IMPORT json.*");
    runStatement("X(@a: Int) := SELECT " + fncName + " AS json FROM jsondata");
    return convert("X");
  }

  @SneakyThrows
  private Pair<Object, Object> convert(String fncName) {
    SqrlTableMacro x = framework.getQueryPlanner().getSchema().getTableFunction(fncName);
    RelNode relNode = x.getViewTransform().get();

    RelNode pgRelNode = framework.getQueryPlanner().convertRelToDialect(Dialect.POSTGRES, relNode);
    String pgQuery = framework.getQueryPlanner().relToString(Dialect.POSTGRES, pgRelNode).getSql();
    snapshot.addContent(pgQuery, "postgres");

    // Execute Postgres query
    Object pgResult = executePostgresQuery(pgQuery);
    //Unbox result
    pgResult = pgResult instanceof PGobject ? ((PGobject) pgResult).getValue() : pgResult;
    pgResult = pgResult == null ? "<null>" : pgResult.toString();

    CreateTableDDL pg = new PostgresDDLFactory().createTable(
        new EngineSink("pg", new int[]{0}, relNode.getRowType(), OptionalInt.of(0), null));

    snapshot.addContent((String) pgResult, "Postgres Result");

    RelNode flinkRelNode = framework.getQueryPlanner().convertRelToDialect(Dialect.FLINK, relNode);
    String query = framework.getQueryPlanner().relToString(Dialect.FLINK, flinkRelNode).getSql();
    snapshot.addContent(query, "flink");

    Object flinkResult = jsonFunctionTest(query);
    if (flinkResult instanceof FlinkJsonType) {
      flinkResult = ((FlinkJsonType) flinkResult).getJson();
    }
    flinkResult = flinkResult == null ? "<null>" : flinkResult.toString();
    snapshot.addContent((String) flinkResult, "Flink Result");
    return Pair.of(pgResult, flinkResult);
  }

  //todo: Hacky way to get different in-mem sources to load
  @AutoService(SourceFactory.class)
  public static class InMemJson extends InMemSourceFactory {

    static Map<String, List<?>> tableData = Map.of("data",
        List.of(new JsonData(1, "{\"example\":[1,2,3]}"),
            new JsonData(2, "{\"example\":[4,5,6]}")));

    public InMemJson() {
      super("data", tableData);
    }
  }

  @Value
  public static class JsonData {

    int id;
    String json;
  }

  @AutoService(DataSystemConnectorFactory.class)
  public static class InMemJsonConnector extends MemoryConnectorFactory {

    public InMemJsonConnector() {
      super("data");
    }
  }
}