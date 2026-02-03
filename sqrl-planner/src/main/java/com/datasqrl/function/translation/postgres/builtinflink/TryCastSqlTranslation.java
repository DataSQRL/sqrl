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
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

/**
 * Rewrites Flink {@code TRY_CAST(value AS type)} into a PostgreSQL-friendly guarded cast.
 *
 * <p>PostgreSQL has no built-in TRY_CAST, so we validate the input first (currently via regex) and
 * only apply {@code CAST} when it looks safe; otherwise return {@code NULL}.
 */
@AutoService(OperatorRuleTransform.class)
public class TryCastSqlTranslation implements OperatorRuleTransform {

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
      return guardedRegexCast(rexBuilder, returnType, value, integerPattern());
    }

    if (sqlTypeName == SqlTypeName.BIGINT) {
      return guardedRegexCast(rexBuilder, returnType, value, integerPattern());
    }

    if (sqlTypeName == SqlTypeName.BOOLEAN) {
      return guardedRegexCast(
          rexBuilder,
          returnType,
          value,
          booleanPattern(),
          SqlStdOperatorTable.POSIX_REGEX_CASE_INSENSITIVE);
    }

    if (sqlTypeName == SqlTypeName.DECIMAL) {
      var precision = targetType.getPrecision();
      var scale = targetType.getScale();
      return guardedRegexCast(rexBuilder, returnType, value, decimalPattern(precision, scale));
    }

    if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      var precision = targetType.getPrecision();
      return guardedRegexCast(rexBuilder, returnType, value, timestampPattern(precision, false));
    }

    if (sqlTypeName == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE) {
      var precision = targetType.getPrecision();
      return guardedRegexCast(rexBuilder, returnType, value, timestampPattern(precision, true));
    }

    if (SqlTypeName.CHAR_TYPES.contains(sqlTypeName)) {
      // Text casts are always safe.
      return rexBuilder.makeCast(returnType, value, true);
    }

    throw new UnsupportedOperationException(
        "TRY_CAST to %s is not supported yet".formatted(sqlTypeName));
  }

  private static RexNode guardedRegexCast(
      RexBuilder rexBuilder, RelDataType returnType, RexNode value, String pattern) {
    return guardedRegexCast(
        rexBuilder, returnType, value, pattern, SqlStdOperatorTable.POSIX_REGEX_CASE_SENSITIVE);
  }

  private static RexNode guardedRegexCast(
      RexBuilder rexBuilder,
      RelDataType returnType,
      RexNode value,
      String pattern,
      SqlOperator regexOperator) {
    var typeFactory = rexBuilder.getTypeFactory();

    // Run validation against a string representation.
    var stringType = typeFactory.createSqlType(SqlTypeName.VARCHAR);
    var valueAsString =
        SqlTypeName.CHAR_TYPES.contains(value.getType().getSqlTypeName())
            ? value
            : rexBuilder.makeCast(stringType, value, true);

    var matches =
        rexBuilder.makeCall(regexOperator, valueAsString, rexBuilder.makeLiteral(pattern));
    var casted = rexBuilder.makeCast(returnType, valueAsString, true);
    var nullLiteral = rexBuilder.makeNullLiteral(returnType);

    // CASE WHEN <matches> THEN <casted> ELSE NULL END
    return rexBuilder.makeCall(
        returnType, SqlStdOperatorTable.CASE, List.of(matches, casted, nullLiteral));
  }

  private static String integerPattern() {
    return "^[+-]?[0-9]+$";
  }

  private static String booleanPattern() {
    return "^(true|false)$";
  }

  private static String decimalPattern(int precision, int scale) {
    // If precision/scale are unspecified, be permissive.
    if (precision <= 0 || scale < 0) {
      return "^[+-]?(?:[0-9]+(?:\\.[0-9]+)?|\\.[0-9]+)$";
    }

    var maxIntegerDigits = Math.max(precision - scale, 0);

    if (scale == 0) {
      // Allow leading zeros.
      return "^[+-]?(?:0*[0-9]{1," + precision + "})$";
    }

    if (maxIntegerDigits == 0) {
      // Must be in (-1, 1), but allow 0.xxx and .xxx
      return "^[+-]?(?:0*(?:\\.[0-9]{1," + scale + "})?|\\.[0-9]{1," + scale + "})$";
    }

    // Integer digits <= maxIntegerDigits, fractional digits <= scale.
    return "^[+-]?(?:0*[0-9]{1,"
        + maxIntegerDigits
        + "}(?:\\.[0-9]{1,"
        + scale
        + "})?|\\.[0-9]{1,"
        + scale
        + "})$";
  }

  private static String timestampPattern(int precision, boolean allowTimezone) {
    // Calcite uses precision for fractional seconds. Default is often 3.
    var fracDigits = precision > 0 ? precision : 6;

    var base = "^\\d{4}-\\d{2}-\\d{2}[ T]\\d{2}:\\d{2}:\\d{2}";
    var fractional = "(?:\\.\\d{1," + fracDigits + "})?";

    if (!allowTimezone) {
      return base + fractional + "$";
    }

    // Accept optional timezone suffix (or none), matching common PG formats.
    var tz = "(?:\\s*(?:Z|[+-]\\d{2}(?::?\\d{2})?))?";
    return base + fractional + tz + "$";
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
