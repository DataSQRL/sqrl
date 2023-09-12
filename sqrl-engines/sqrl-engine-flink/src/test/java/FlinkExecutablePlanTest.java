import static com.datasqrl.functions.TimeFunctions.EPOCH_TO_TIMESTAMP;
import static com.datasqrl.functions.TimeFunctions.TIMESTAMP_TO_EPOCH;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.FlinkExecutablePlan.*;
import com.datasqrl.FlinkEnvironmentBuilder;
import com.datasqrl.model.LogicalStreamMetaData;
import com.datasqrl.serializer.SerializableSchema;
import com.datasqrl.serializer.SerializableSchema.WaterMarkType;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import com.datasqrl.model.StreamType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.StatementSet;
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
                        + "  AS '"+ TIMESTAMP_TO_EPOCH.getClass().getName() + "'"
                        + " LANGUAGE JAVA")
                    .build(),
                FlinkSqlFunction.builder()
                    .functionSql("CREATE TEMPORARY FUNCTION \n"
                        + "  EPOCH_TO_TIMESTAMP \n"
                        + "  AS '"+ EPOCH_TO_TIMESTAMP.getClass().getName() + "'"
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
                    .schema(SerializableSchema.builder()
                        .column(Pair.of("id", DataTypes.STRING()))
                        .column(Pair.of("updatedTime", DataTypes.TIMESTAMP_LTZ()))
                        .column(Pair.of("type", DataTypes.STRING()))
                        .waterMarkType(WaterMarkType.NONE)
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

    FlinkEnvironmentBuilder visitor = new FlinkEnvironmentBuilder();
    StatementSet statementSet = executablePlan.accept(visitor, null);
    TableResult result = statementSet.execute();

    result.print();

    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, result.getResultKind());
  }
}