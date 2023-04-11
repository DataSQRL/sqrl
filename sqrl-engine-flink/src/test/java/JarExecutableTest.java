import static org.junit.jupiter.api.Assertions.assertEquals;

import com.datasqrl.FlinkExecutablePlan;
import com.datasqrl.FlinkExecutablePlan.DefaultFlinkConfig;
import com.datasqrl.FlinkExecutablePlan.FlinkBase;
import com.datasqrl.FlinkExecutablePlan.FlinkJarStatement;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlFunction;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlQuery;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlSink;
import com.datasqrl.FlinkExecutablePlan.FlinkSqlTableApiDefinition;
import com.datasqrl.FlinkEnvironmentBuilder;
import com.google.common.io.Resources;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.ResultKind;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.TableResult;
import org.junit.jupiter.api.Test;

public class JarExecutableTest {


  @Test
  public void testJar() {
    FlinkExecutablePlan executablePlan = FlinkExecutablePlan.builder()
        .base(FlinkBase.builder()
            .config(DefaultFlinkConfig.builder()
                .streamExecutionEnvironmentConfig(Map.of())
                .tableEnvironmentConfig(Map.of())
                .build())
            .statements(List.of(FlinkJarStatement.builder()
                    .path(Resources.getResource("myudf-test.jar").getFile())
                .build()))
            .functions(List.of(
                FlinkSqlFunction.builder()
                    .functionSql("CREATE TEMPORARY FUNCTION \n"
                        + "  MyScalarFunction \n"
                        + "  AS 'com.myudf.MyScalarFunction'"
                        + " LANGUAGE JAVA")
                    .build()
            ))
            .tableDefinitions(List.of(
                FlinkSqlTableApiDefinition.builder()
                    .createSql("CREATE TABLE MyTable (\n"
                        + "      id BIGINT NOT NULL"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'number-of-rows' = '10'"
                        + ")")
                    .build(),
                FlinkSqlTableApiDefinition.builder()
                    .createSql("CREATE TABLE sink$1 (\n"
                        + "  id BIGINT NOT NULL,"
                        + "  fnc BIGINT NOT NULL"
                        + ") WITH (\n"
                        + "  'connector' = 'print'\n"
                        + ");")
                    .build()))
            .queries(List.of(
                FlinkSqlQuery.builder()
                    .name("table$1")
                    .query("SELECT id, MyScalarFunction(id, id) AS fnc FROM MyTable")
                    .build()
            ))
            .sinks(List.of(FlinkSqlSink.builder()
                .source("table$1")
                .target("sink$1")
                .build()))
            .build())
        .build();

    FlinkEnvironmentBuilder planVisitor = new FlinkEnvironmentBuilder();
    StatementSet statementSet = executablePlan.accept(planVisitor, null);
    TableResult result = statementSet.execute();

    result.print();

    assertEquals(ResultKind.SUCCESS_WITH_CONTENT, result.getResultKind());
  }
}
