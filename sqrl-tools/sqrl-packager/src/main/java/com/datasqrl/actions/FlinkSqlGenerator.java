package com.datasqrl.actions;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.calcite.convert.SqlToStringFactory;
import com.datasqrl.engine.stream.flink.plan.FlinkStreamPhysicalPlan;
import com.datasqrl.engine.stream.flink.plan.SqrlToFlinkSqlGenerator;
import com.datasqrl.engine.stream.flink.plan.SqrlToFlinkSqlGenerator.SqlResult;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlSet;
import org.apache.flink.sql.parser.dml.SqlExecute;
import org.apache.flink.sql.parser.dml.SqlStatementSet;

/**
 *
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class FlinkSqlGenerator {

  private final SqrlFramework framework;

  public List<String> run(FlinkStreamPhysicalPlan physicalPlan) {
    SqrlToFlinkSqlGenerator sqlPlanner = new SqrlToFlinkSqlGenerator(framework);
    SqlResult result = sqlPlanner.plan(physicalPlan.getPlan());

    List<SqlNode> flinkSql = new ArrayList<>();
    flinkSql.addAll(framework.getSchema().getAddlSql());
    flinkSql.addAll(result.getFunctions());
    flinkSql.addAll(result.getSinksSources());
    flinkSql.addAll(result.getQueries());
    SqlStatementSet sqlStatementSet = new SqlStatementSet(result.getInserts(), SqlParserPos.ZERO);
    SqlExecute execute = new SqlExecute(sqlStatementSet, SqlParserPos.ZERO);
    flinkSql.add(execute);

    SqlNodeToString sqlNodeToString = SqlToStringFactory.get(Dialect.CALCITE);
    Map<String, String> config = new LinkedHashMap<>();
    List<String> plan = new ArrayList<>();
    for (SqlNode sqlNode : flinkSql) {
      if (sqlNode instanceof SqlSet) {
        SqlSet set = (SqlSet) sqlNode;
        config.put(set.getKeyString(), set.getValueString());
      } else {
        plan.add(sqlNodeToString.convert(() -> sqlNode).getSql() + ";");
      }
    }
    return plan;

  }
}
