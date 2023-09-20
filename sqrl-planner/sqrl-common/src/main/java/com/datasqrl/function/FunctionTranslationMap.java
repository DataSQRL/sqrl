package com.datasqrl.function;

import com.datasqrl.FunctionUtil;
import com.datasqrl.TsVectorOperatorTable;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform;
import com.datasqrl.calcite.convert.SimplePredicateTransform;
import com.datasqrl.calcite.function.RuleTransform;
import com.datasqrl.canonicalizer.Name;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class FunctionTranslationMap {

  public static final Map<String, RuleTransform> transformMap = Map.of(
      "textsearch", new TextSearchTranslation(),
      "now", new NowTranslation()
  );

  public static class NowTranslation implements RuleTransform {

    @Override
    public List<RelRule> transform(Dialect dialect, SqlOperator operator) {
      if (dialect == Dialect.POSTGRES) {
        return List.of(new SimplePredicateTransform(operator,
            (rexBuilder, predicate) -> rexBuilder.makeCall(SqlStdOperatorTable.CURRENT_TIMESTAMP)));
      }

      return List.of();
    }

  }

  public static class TextSearchTranslation implements RuleTransform {


    @Override
    public List<RelRule> transform(Dialect dialect, SqlOperator operator) {
      if (dialect != Dialect.POSTGRES) {
        return List.of();
      }

      return List.of(
          new SimplePredicateTransform(operator, (rexBuilder, predicate) -> {
            Preconditions.checkArgument(predicate.isA(SqlKind.BINARY_COMPARISON) && predicate.getOperands().size()==2,
                "Expected %s in comparison predicate but got: %s",
                getFunctionName(), predicate);
            RexNode other;
            RexCall textSearch;
            if (predicate.getOperands().get(0) instanceof RexCall) {
              textSearch = (RexCall) predicate.getOperands().get(0);
              other = predicate.getOperands().get(1);
              Preconditions.checkArgument(predicate.isA(SqlKind.GREATER_THAN),
                  "Expected a greater-than (>) comparison in %s predicate but got %s",
                  getFunctionName(), predicate);
            } else if (predicate.getOperands().get(1) instanceof RexCall) {
              textSearch = (RexCall) predicate.getOperands().get(1);
              other = predicate.getOperands().get(0);
              Preconditions.checkArgument(predicate.isA(SqlKind.LESS_THAN),
                  "Expected a less-than (<) comparison in %s predicate but got %s",
                  getFunctionName(), predicate);
            } else {
              throw new IllegalArgumentException("Not a valid predicate");
            }
            Preconditions.checkArgument(FunctionUtil.getSqrlFunction(textSearch.getOperator())
                    .filter(fct -> fct.getFunctionName().equals(Name.system("TextSearch"))).isPresent(),
                "Not a valid %s predicate", getFunctionName());
            //TODO generalize to other literals by adding ts_rank_cd to the filter condition
            Preconditions.checkArgument(other instanceof RexLiteral &&
                ((RexLiteral)other).getValueAs(Number.class).doubleValue()==0, "Expected comparison with 0 for %s", getFunctionName());
            //TODO: allow other languages
            RexLiteral language = rexBuilder.makeLiteral("english");
            List<RexNode> operands = textSearch.getOperands();
            Preconditions.checkArgument(operands.size()>1);

            //to_tsvector(col1  ' '  coalesce(col2,'')) @@ to_tsquery(:query) AND ts_rank_cd(col1..., :query) > 0.1
            return rexBuilder.makeCall(TsVectorOperatorTable.MATCH, makeTsVector(rexBuilder, language, operands.subList(1,
                operands.size())), makeTsQuery(rexBuilder, language, operands.get(0)));
          }),
          new SimpleCallTransform(operator, (rexBuilder, call) -> {
            Preconditions.checkArgument(FunctionUtil.getSqrlFunction(call.getOperator())
                    .filter(fct -> fct.getFunctionName().equals(Name.system("TextSearch"))).isPresent(),
                "Not a valid %s predicate", getFunctionName());
            RexLiteral language = rexBuilder.makeLiteral("english");
            List<RexNode> operands = call.getOperands();
            Preconditions.checkArgument(operands.size()>1);
            return rexBuilder.makeCall(TsVectorOperatorTable.TS_RANK_CD, makeTsVector(rexBuilder, language, operands.subList(1,
                operands.size())), makeTsQuery(rexBuilder, language, operands.get(0)));
          })
      );
    }

    private String getFunctionName() {
      return "TextSearch";
    }

    private RexNode makeTsQuery(RexBuilder rexBuilder, RexLiteral language, RexNode query) {
      return rexBuilder.makeCall(TsVectorOperatorTable.TO_WEBQUERY, language, query);
    }

    private RexNode makeTsVector(RexBuilder rexBuilder, RexLiteral language, List<RexNode> columns) {
      RexLiteral space = rexBuilder.makeLiteral(" ");
      List<RexNode> args = columns.stream().map(col -> rexBuilder.makeCall(SqlStdOperatorTable.COALESCE,col, space))
          .collect(Collectors.toList());
      RexNode arg;
      if (args.size()>1) {
        arg = args.get(0);
        for (int i = 1; i < args.size(); i++) {
          arg = rexBuilder.makeCall(SqlStdOperatorTable.CONCAT, rexBuilder.makeCall(
              SqlStdOperatorTable.CONCAT, arg, space), args.get(i));
        }
      } else {
        arg = args.get(0);
      }
      return rexBuilder.makeCall(TsVectorOperatorTable.TO_TSVECTOR, language, arg);
    }

  }

}
