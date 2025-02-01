package com.datasqrl.functions.json.postgres;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;
import static com.datasqrl.function.PgSpecificOperatorTable.JsonToString;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform.SimpleCallTransformConfig;
import com.datasqrl.calcite.function.OperatorRuleTransform;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Extracts a scalar value based on a JSON path.
 */
@AutoService(OperatorRuleTransform.class)
public class JsonExtractTranslation implements OperatorRuleTransform {

  public static final SqlFunction PG_JSONB_PATH_QUERY_FIRST = lightweightOp(
      "jsonb_path_query_first");

  @Override
  public List<RelRule> transform(Dialect dialect, SqlOperator operator) {
    if (dialect == Dialect.POSTGRES) {
      return postgresTransform(operator);
    }
    return List.of();
  }

  @Override
  public String getRuleOperatorName() {
    return "jsonextract";
  }

  private List<RelRule> postgresTransform(SqlOperator operator) {
    return List.of((RelRule) SimpleCallTransformConfig.createConfig(operator, (rexBuilder, call) -> {
      List<RexNode> operands = new ArrayList<>(call.getOperands());
      if (call.getOperands().size() == 3 && call.getOperands().get(2).getType().getSqlTypeName() != SqlTypeName.NULL) {

        RexNode query = rexBuilder.getRexBuilder().makeCall(
            rexBuilder.getRexBuilder().getTypeFactory().createSqlType(SqlTypeName.ANY),
            PG_JSONB_PATH_QUERY_FIRST, operands.subList(0, 2));

        RelDataType type = call.getOperands().get(2).getType();

        // Strings would otherwise come back as quoted strings unless we cast to string with the jsonb function
        RexNode op1ToType;
        if (SqlTypeName.CHAR_TYPES.contains(type.getSqlTypeName())) {
          op1ToType = rexBuilder.getRexBuilder().makeCall(JsonToString, query,
              rexBuilder.getRexBuilder().makeLiteral("{}"));
        } else {
          op1ToType = rexBuilder.getRexBuilder().makeCast(type, query, true);
        }

        RexNode defaultValue = call.getOperands().get(2);

        return rexBuilder.getRexBuilder().makeCall(rexBuilder.getRexBuilder().getTypeFactory().createSqlType(SqlTypeName.ANY),
            SqlStdOperatorTable.COALESCE, List.of(op1ToType, defaultValue));
      }

      RexNode query = rexBuilder.getRexBuilder().makeCall(
          rexBuilder.getRexBuilder().getTypeFactory().createSqlType(SqlTypeName.ANY),
          PG_JSONB_PATH_QUERY_FIRST, operands.subList(0, 2));
      return rexBuilder.getRexBuilder().makeCall(JsonToString, query,
          rexBuilder.getRexBuilder().makeLiteral("{}"));
    }).toRule());
  }
}
