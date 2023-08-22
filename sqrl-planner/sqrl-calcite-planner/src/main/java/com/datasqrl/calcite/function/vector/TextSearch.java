package com.datasqrl.calcite.function.vector;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform;
import com.datasqrl.calcite.function.RuleTransform;
import com.datasqrl.calcite.convert.SimplePredicateTransform;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.List;

public class TextSearch extends ScalarFunction implements RuleTransform {
  public Double eval(String query, String... texts) {
    return 0.0;
  }

  @Override
  public List<RelRule> transform(Dialect dialect, SqlOperator operator) {
    if (dialect != Dialect.POSTGRES) {
      return List.of();
    }

    return List.of(
        new SimplePredicateTransform(operator, (rexBuilder, predicate) -> {
          RexCall call = (RexCall) predicate.getOperands().get(0);
          //to_tsvector(col1  ' '  coalesce(col2,'')) @@ to_tsquery(:query) AND ts_rank_cd(col1..., :query) > 0.1
          return rexBuilder.makeCall(TsVectorOperatorTable.MATCH,
              rexBuilder.makeCall(TsVectorOperatorTable.TO_TSVECTOR, call.getOperands().get(1)),
              rexBuilder.makeCall(SqlStdOperatorTable.AND,
                  rexBuilder.makeCall(TsVectorOperatorTable.TO_TSQUERY, call.getOperands().get(0)),
                  rexBuilder.makeCall(predicate.getOperator(),
                      rexBuilder.makeCall(TsVectorOperatorTable.TS_RANK_CD, call.getOperands().get(1), call.getOperands().get(0)),
                      predicate.getOperands().get(1))));
        }),
        new SimpleCallTransform(operator, (rexBuilder, call) ->
            rexBuilder.makeCall(TsVectorOperatorTable.TS_RANK_CD, call.getOperands().get(1), call.getOperands().get(0))));
  }
}
