package datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import com.datasqrl.io.schema.flexible.input.external.TableDefinition;
import com.datasqrl.serializer.Deserializer;

import lombok.SneakyThrows;

@ExtendWith(MiniClusterExtension.class)
class FlexibleSchemaTest {

  @SneakyThrows
  @Test
  @Disabled
  public void testFlink() {
    String schema = getSchema(Path.of(
        "../sqrl-examples/conference/mysourcepackage/authtokens.schema.yml"));

    String tableSql = """
CREATE TABLE MyUserTable (
  `id` STRING NOT NULL,
  `value` STRING NOT NULL,
  `last_updated` TIMESTAMP_LTZ NOT NULL
) WITH (
  'connector' = 'filesystem',         \s
  'path' = '../../sqrl-examples/conference/data/authtokens.json',\s
  'format' = 'flexible-json'
)""";

    List<Row> rows = test(tableSql,
        "SELECT * FROM MyUserTable");
    assertEquals(1, rows.size());
  }

  @SneakyThrows
  @Test
  @Disabled
  public void
  testFlinkCsv() {
    String schema = getSchema(Path.of(
        "../../sqrl-examples/quickstart/mysourcepackage/products.schema.yml"));

    String tableSql = """
        CREATE TABLE MyUserTable (
          `id` STRING NOT NULL,
          `name` STRING NOT NULL,
          `sizing` STRING NOT NULL,
          `weight_in_gram` BIGINT NOT NULL,
          `type` STRING NOT NULL,
          `category` STRING NOT NULL,
          `usda_id` BIGINT NOT NULL,
          `updated` STRING NOT NULL\
        ) WITH (
          'connector' = 'filesystem',         \s
          'path' = '../../sqrl-examples/quickstart/data/products.csv.gz',\s
          'format' = 'flexible-csv',
          'flexible-csv.skip-header' = 'true',
          'flexible-csv.ignore-parse-errors' = 'true',
          'flexible-csv.allow-comments' = 'true',
          'flexible-csv.field-delimiter' = ';'
        )""";

    List<Row> rows = test(tableSql,
        "SELECT * FROM MyUserTable");
    assertEquals(25, rows.size());
  }

  @SneakyThrows
  public String getSchema(Path path) {
    Deserializer deserializer = Deserializer.INSTANCE;
    TableDefinition tableDefinition = deserializer.mapYAMLFile(path,
        TableDefinition.class);
    String json = deserializer.getJsonMapper().writeValueAsString(tableDefinition);

    return json;
  }

  public List<Row> test(String tableSql, String sql) {
    var env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    var tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(tableSql);

    var tableResult = tableEnv.executeSql(sql);

    List<Row> rows = new ArrayList<>();
    tableResult.collect().forEachRemaining(rows::add);
    return rows;
  }

}