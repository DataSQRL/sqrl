package com.datasqrl.actions;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.calcite.convert.SqlToStringFactory;
import com.datasqrl.engine.stream.flink.plan.SqrlToFlinkSqlGenerator;
import com.datasqrl.engine.stream.flink.plan.SqrlToFlinkSqlGenerator.SqlResult;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StreamStagePlan;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.collections.ListUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlStatementSet;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.CompiledPlan;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.operations.StatementSetOperation;

/**
 *
 */
@AllArgsConstructor(onConstructor_ = @Inject)
@Slf4j
public class FlinkSqlGenerator {

  private final SqrlFramework framework;

  public FlinkSqlGeneratorResult run(StreamStagePlan physicalPlan,
      List<StagePlan> stagePlans) {
    SqrlToFlinkSqlGenerator sqlPlanner = new SqrlToFlinkSqlGenerator(framework);
    SqlResult result = sqlPlanner.plan(physicalPlan.getQueries(), stagePlans);

    List<SqlNode> flinkSql = new ArrayList<>();
    flinkSql.addAll(framework.getSchema().getAddlSql());
    flinkSql.addAll(result.getFunctions());
    flinkSql.addAll(result.getSinksSources());
    flinkSql.addAll(result.getQueries());
    if (result.getInserts().isEmpty()){
      throw new RuntimeException("Flink stage empty. No queries or exports were found.");
    }
    SqlStatementSet sqlStatementSet = new SqlStatementSet(result.getInserts(), SqlParserPos.ZERO);
    SqlExecute execute = new SqlExecute(sqlStatementSet, SqlParserPos.ZERO);
    flinkSql.add(execute);

    SqlNodeToString sqlNodeToString = SqlToStringFactory.get(Dialect.FLINK);
    Map<String, String> config = new LinkedHashMap<>();
    List<String> plan = new ArrayList<>();
    for (SqlNode sqlNode : flinkSql) {
      if (sqlNode instanceof SqlSet) {
        SqlSet set = (SqlSet) sqlNode;
        config.put(set.getKeyString(), set.getValueString());
      } else {
        String sql = sqlNodeToString.convert(() -> sqlNode).getSql() + ";";
        plan.add(sql);
      }
    }

    CompiledPlan compiledPlan = null;
    try {
      compiledPlan = createCompiledPlan(result);
    } catch (Exception e) {
      log.error("Could not prepare compiled plan", e);
    }

      return new FlinkSqlGeneratorResult(plan, flinkSql, compiledPlan);
  }

  private CompiledPlan createCompiledPlan(SqlResult result) {
    List<SqlNode> stubSchema = result.getStubSchema();
    stubSchema = ListUtils.union(stubSchema, result.getQueries());

    StreamExecutionEnvironment sEnv;

    sEnv = StreamExecutionEnvironment.getExecutionEnvironment(Configuration.fromMap(Map.of()));

    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(Map.of()))
        .build();

    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
    TableResult tableResult = null;
    SqlNodeToString sqlNodeToString = SqlToStringFactory.get(Dialect.FLINK);

    for (int i = 0; i < stubSchema.size(); i++) {
      SqlNode sqlNode = stubSchema.get(i);
      String statement = sqlNodeToString.convert(() -> sqlNode).getSql() + ";";

      try {
        tableResult = tEnv.executeSql(statement);
      } catch (Exception e) {
        System.out.println("Could not execute statement: " + statement);
        throw e;
      }
    }
    SqlStatementSet sqlStatementSet = new SqlStatementSet(result.getInserts(), SqlParserPos.ZERO);
    SqlExecute execute = new SqlExecute(sqlStatementSet, SqlParserPos.ZERO);

    String insert = sqlNodeToString.convert(() -> execute).getSql() + ";";

    TableEnvironmentImpl tEnv1 = (TableEnvironmentImpl) tEnv;

    StatementSetOperation parse = (StatementSetOperation)tEnv1.getParser().parse(insert).get(0);

    return tEnv1.compilePlan(parse.getOperations());
  }

  @Value
  public class FlinkSqlGeneratorResult {
    List<String> plan;
    List<SqlNode> flinkSql;
    CompiledPlan compiledPlan;
  }
}
