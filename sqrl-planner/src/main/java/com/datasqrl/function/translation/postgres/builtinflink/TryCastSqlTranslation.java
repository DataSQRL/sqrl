/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.function.translation.postgres.builtinflink;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;
import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform;
import com.datasqrl.calcite.function.OperatorRuleTransform;
import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

/**
 * Rewrites Flink {@code TRY_CAST(value AS type)} into a PostgreSQL-friendly guarded cast.
 *
 * <p>PostgreSQL has no built-in TRY_CAST, so we validate the input first (via {@code
 * pg_input_is_valid}) and only apply {@code CAST} when it looks safe; otherwise return {@code
 * NULL}.
 */
@AutoService(OperatorRuleTransform.class)
public class TryCastSqlTranslation implements OperatorRuleTransform {

  private static final SqlUnresolvedFunction PG_INPUT_IS_VALID = lightweightOp("pg_input_is_valid");

  @Override
  public List<RelRule> transform(SqlOperator operator) {
    return List.of(
        (RelRule)
            SimpleCallTransform.SimpleCallTransformConfig.createConfig(
                    operator,
                    (relBuilder, call) -> {
                      var operands = call.getOperands();
                      checkArgument(operands.size() == 1);

                      var value = operands.get(0);
                      var targetType = call.getType();
                      return rewriteTryCast(relBuilder, value, targetType);
                    })
                .toRule());
  }

  private static RexNode rewriteTryCast(
      RelBuilder relBuilder, RexNode value, RelDataType targetType) {
    var rexBuilder = relBuilder.getRexBuilder();
    var typeFactory = rexBuilder.getTypeFactory();

    // TRY_CAST always needs to be nullable (cast failures return NULL)
    var returnType = typeFactory.createTypeWithNullability(targetType, true);

    var sqlTypeName = targetType.getSqlTypeName();
    if (sqlTypeName == SqlTypeName.INTEGER) {
      return guardedPgInputIsValidCast(rexBuilder, returnType, value, "integer");
    }

    if (sqlTypeName == SqlTypeName.BIGINT) {
      return guardedPgInputIsValidCast(rexBuilder, returnType, value, "bigint");
    }

    if (sqlTypeName == SqlTypeName.BOOLEAN) {
      return guardedPgInputIsValidCast(rexBuilder, returnType, value, "boolean");
    }

    if (sqlTypeName == SqlTypeName.DECIMAL) {
      return guardedPgInputIsValidCast(rexBuilder, returnType, value, "numeric");
    }

    if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      return guardedPgInputIsValidCast(rexBuilder, returnType, value, "timestamp");
    }

    if (sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      return guardedPgInputIsValidCast(rexBuilder, returnType, value, "timestamptz");
    }

    if (SqlTypeName.CHAR_TYPES.contains(sqlTypeName)) {
      // Text casts are always safe.
      return rexBuilder.makeCast(returnType, value, true);
    }

    throw new UnsupportedOperationException(
        "TRY_CAST to %s is not supported yet".formatted(sqlTypeName));
  }

  private static RexNode guardedPgInputIsValidCast(
      RexBuilder rexBuilder, RelDataType returnType, RexNode value, String pgTypeName) {
    var typeFactory = rexBuilder.getTypeFactory();

    // pg_input_is_valid expects (text, regtype)
    var stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    var valueAsString =
        SqlTypeName.CHAR_TYPES.contains(value.getType().getSqlTypeName())
            ? value
            : rexBuilder.makeCast(stringType, value, true);

    var booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    var isValid =
        rexBuilder.makeCall(
            booleanType,
            PG_INPUT_IS_VALID,
            List.of(valueAsString, rexBuilder.makeLiteral(pgTypeName)));

    var casted = rexBuilder.makeCast(returnType, valueAsString, true);
    var nullLiteral = rexBuilder.makeNullLiteral(returnType);

    // CASE WHEN <isValid> THEN <casted> ELSE NULL END
    return rexBuilder.makeCall(
        returnType, SqlStdOperatorTable.CASE, List.of(isValid, casted, nullLiteral));
  }

  @Override
  public Dialect getDialect() {
    return Dialect.POSTGRES;
  }

  @Override
  public String getRuleOperatorName() {
    return "try_cast";
  }
}
