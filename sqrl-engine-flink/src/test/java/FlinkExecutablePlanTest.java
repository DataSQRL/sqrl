import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.FlinkExecutablePlan.*;
import com.datasqrl.JavaFlinkExecutablePlanVisitor;
import com.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import com.datasqrl.plan.calcite.rel.LogicalStreamMetaData;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import org.apache.calcite.sql.StreamType;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;

class FlinkExecutablePlanTest {


  @SneakyThrows
  @Test
  public void test() {
    FlinkExecutablePlan executablePlan = FlinkExecutablePlan.builder()
        .base(FlinkBase.builder()
            .config(DefaultFlinkConfig.builder()
                .streamExecutionEnvironmentConfig(Map.of())
                .tableEnvironmentConfig(Map.of())
                .build())
            .functions(List.of(
                FlinkSqlFunction.builder()
                    .functionSql("CREATE TEMPORARY FUNCTION \n"
                        + "  TIMESTAMP_TO_EPOCH \n"
                        + "  AS '"+ StdTimeLibraryImpl.TIMESTAMP_TO_EPOCH.getClass().getName() + "'"
                        + " LANGUAGE JAVA")
                    .build(),
                FlinkSqlFunction.builder()
                    .functionSql("CREATE TEMPORARY FUNCTION \n"
                        + "  EPOCH_TO_TIMESTAMP \n"
                        + "  AS '"+ StdTimeLibraryImpl.EPOCH_TO_TIMESTAMP.getClass().getName() + "'"
                        + " LANGUAGE JAVA")
                    .build()
            ))
            .tableDefinitions(List.of(
                FlinkSqlTableApiDefinition.builder()
                    .createSql("CREATE TABLE Products (\n"
                        + "      id INT,\n"
                        + "      name STRING,\n"
                        + "      sizing STRING,\n"
                        + "      weight_in_gram INT,\n"
                        + "      type STRING,\n"
                        + "      category STRING,\n"
                        + "      usda_id INT,\n"
                        + "      updated TIMESTAMP_LTZ\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '10'"
                        + ")")
                    .build(),
                FlinkSqlTableApiDefinition.builder()
                    .createSql("CREATE TABLE sink$1 (\n"
                        + "  t STRING,"
                        + "  ts BIGINT"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ");")
                    .build()
            ))
            .queries(List.of(
                FlinkSqlQuery.builder()
                  .name("product$1")
                  .query("SELECT id, updated, type FROM Products")
                  .build(),
                FlinkStreamQuery.builder()
                  .fromTable("product$1")
                  .meta(new LogicalStreamMetaData(new int[]{0}, new int[]{2}, 1))
                  .stateChangeType(StreamType.ADD)
                  .unmodifiedChangelog(false)
                  .typeInformation(new RowTypeInfo(
                      new TypeInformation[] {
                          BasicTypeInfo.STRING_TYPE_INFO,
                          BasicTypeInfo.INSTANT_TYPE_INFO,
                          BasicTypeInfo.STRING_TYPE_INFO,
                      },
                      new String[] {
                          "id","updatedTime","type"
                      }
                  ))
                    .schema(Schema.newBuilder()
                        .column("id", DataTypes.STRING())
                        .column("updatedTime", DataTypes.TIMESTAMP_LTZ())
                        .column("type", DataTypes.STRING())
                        .build())
                  .name("product$stream")
                  .build()
                ,
                FlinkSqlQuery.builder()
                    .name("product$2")
                    .query("SELECT type AS t, TIMESTAMP_TO_EPOCH(updatedTime) AS ts FROM product$stream")
                    .build()
                ))
            .sinks(List.of(FlinkSqlSink.builder()
                .source("product$2")
                .target("sink$1")
                .build()))
            .build())
        .build();

    JavaFlinkExecutablePlanVisitor visitor = new JavaFlinkExecutablePlanVisitor();
    TableResult result = executablePlan.accept(visitor, null);

    result.print();

    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, result.getResultKind());
  }
}