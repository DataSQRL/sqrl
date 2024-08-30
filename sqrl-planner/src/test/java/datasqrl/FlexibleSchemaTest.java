package datasqrl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.schema.input.external.TableDefinition;
import com.datasqrl.serializer.Deserializer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.types.Row;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MiniClusterExtension.class)
class FlexibleSchemaTest {

  @SneakyThrows
  @Test
  @Disabled
  public void testFlink() {
    String schema = getSchema(Path.of(
        "../sqrl-examples/conference/mysourcepackage/authtokens.schema.yml"));

    String tableSql = "CREATE TABLE MyUserTable (\n"
        + "  `id` STRING NOT NULL,\n"
        + "  `value` STRING NOT NULL,\n"
        + "  `last_updated` TIMESTAMP_LTZ NOT NULL\n"
        + ") WITH (\n"
        + "  'connector' = 'filesystem',          \n"
        + "  'path' = '../../sqrl-examples/conference/data/authtokens.json', \n"
        + "  'format' = 'flexible-json'\n"
//        + "  'flexible-json.schema' = '"+schema+"'   \n"
        + ")";

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

    String tableSql = "CREATE TABLE MyUserTable (\n"
        + "  `id` STRING NOT NULL,\n"
        + "  `name` STRING NOT NULL,\n"
        + "  `sizing` STRING NOT NULL,\n"
        + "  `weight_in_gram` BIGINT NOT NULL,\n"
        + "  `type` STRING NOT NULL,\n"
        + "  `category` STRING NOT NULL,\n"
        + "  `usda_id` BIGINT NOT NULL,\n"
        + "  `updated` STRING NOT NULL"
        + ") WITH (\n"
        + "  'connector' = 'filesystem',          \n"
        + "  'path' = '../../sqrl-examples/quickstart/data/products.csv.gz', \n"
        + "  'format' = 'flexible-csv',\n"
        + "  'flexible-csv.skip-header' = 'true',\n"
        + "  'flexible-csv.ignore-parse-errors' = 'true',\n"
        + "  'flexible-csv.allow-comments' = 'true',\n"
        + "  'flexible-csv.field-delimiter' = ';'\n"
        + ")";

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
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setParallelism(1);

    StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
    tableEnv.executeSql(tableSql);

    TableResult tableResult = tableEnv.executeSql(sql);

    List<Row> rows = new ArrayList<>();
    tableResult.collect().forEachRemaining(rows::add);
    return rows;
  }

}