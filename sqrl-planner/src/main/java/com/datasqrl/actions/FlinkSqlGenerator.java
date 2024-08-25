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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlStatementSet;

/**
 *
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class FlinkSqlGenerator {

  private final SqrlFramework framework;

  public Pair<List<String>, List<SqlNode>> run(StreamStagePlan physicalPlan,
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
    return Pair.of(plan, flinkSql);

  }
}
