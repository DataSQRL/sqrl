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
package com.datasqrl.function.translation.duckdb.builtin;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform;
import com.datasqrl.calcite.function.OperatorRuleTransform;
import com.google.auto.service.AutoService;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;

/**
 * Rewrites the Calcite-decomposed TIMESTAMPDIFF expression tree back into a DuckDB-native {@code
 * date_diff('unit', start, end)} call.
 *
 * <p>Calcite's {@code TimestampDiffConvertlet} decomposes {@code TIMESTAMPDIFF(DAY, ts1, ts2)} into
 * {@code multiplyDivide(CAST(Reinterpret(MINUS_DATE(ts2, ts1))), multiplier, divisor)}. The time
 * unit is encoded as the divisor constant. This rule detects that pattern and reconstructs it.
 *
 * <p>WEEK and QUARTER are emitted by Calcite as a SECOND/MONTH diff wrapped in an extra {@code
 * DIVIDE_INTEGER}. A second rule recognises {@code /INT(date_diff(unit, …), N)} and collapses it
 * into {@code date_diff(largerUnit, …)}.
 */
@AutoService(OperatorRuleTransform.class)
public class TimestampDiffSqlTranslation implements OperatorRuleTransform {

  private static final SqlOperator DATE_DIFF =
      new SqlSpecialOperator(
          "date_diff",
          SqlKind.OTHER_FUNCTION,
          0,
          false,
          ReturnTypes.explicit(SqlTypeName.INTEGER),
          null,
          null) {
        @Override
        public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
          var frame = writer.startFunCall("date_diff");
          writer.print("'" + ((SqlLiteral) call.operand(0)).toValue() + "'");
          writer.sep(",", true);
          call.operand(1).unparse(writer, 0, 0);
          writer.sep(",", true);
          call.operand(2).unparse(writer, 0, 0);
          writer.endFunCall(frame);
        }
      };

  @Override
  public List<RelRule> transform(SqlOperator operator) {
    return List.of(
        // The outermost node is always CAST — Calcite produces either:
        //   CAST(DIVIDE_INTEGER(Reinterpret(MINUS_DATE(ts2, ts1)), divisor)) for most units
        //   CAST(Reinterpret(MINUS_DATE(ts2, ts1))) when multiplier == divisor (SECOND, MONTH)
        (RelRule)
            SimpleCallTransform.SimpleCallTransformConfig.createConfig(
                    SqlStdOperatorTable.CAST, TimestampDiffSqlTranslation::rewriteCast)
                .toRule(),
        // Collapses /INT(date_diff(unit, a, b), N) → date_diff(largerUnit, a, b) for WEEK/QUARTER.
        (RelRule)
            SimpleCallTransform.SimpleCallTransformConfig.createConfig(
                    SqlStdOperatorTable.DIVIDE_INTEGER, TimestampDiffSqlTranslation::rewriteDivide)
                .toRule());
  }

  @Override
  public Dialect getDialect() {
    return Dialect.DUCKDB;
  }

  @Override
  public String getRuleOperatorName() {
    return "reinterpret";
  }

  /**
   * Rewrites Calcite's TIMESTAMPDIFF decomposition back into {@code date_diff('unit', ts1, ts2)}.
   * Walks through any number of nested {@code CAST}/{@code DIVIDE_INTEGER} layers, collecting the
   * divisors, then resolves the final unit by applying the divisors innermost-first against the
   * unified {@code (innerUnit, divisor) → outerUnit} table. This handles:
   *
   * <ul>
   *   <li>{@code CAST(/INT(Reinterpret, divisor))} — most units (DAY, HOUR, MINUTE, SECOND, YEAR).
   *   <li>{@code CAST(Reinterpret)} — bare reinterpret (MONTH); unit comes from the qualifier.
   *   <li>{@code CAST(/INT(CAST(/INT(Reinterpret, d1)), d2))} — Calcite's split decomposition for
   *       WEEK and (when wrapped in an outer user CAST) QUARTER, plus an extra outer user CAST.
   * </ul>
   */
  private static RexNode rewriteCast(RelBuilder relBuilder, RexCall call) {
    if (call.getOperands().isEmpty()) {
      return call;
    }
    if (!(call.getOperands().get(0) instanceof RexCall current)) {
      return call;
    }

    var divisors = new ArrayList<BigDecimal>();
    while (true) {
      // Unwrap consecutive CASTs — Calcite layers them between intermediate /INT operations.
      while (current.getKind() == SqlKind.CAST
          && !current.getOperands().isEmpty()
          && current.getOperands().get(0) instanceof RexCall nested) {
        current = nested;
      }

      // Record the divisor of an /INT layer and descend into its left operand.
      if (current.getKind() == SqlKind.DIVIDE
          && current.getOperands().size() == 2
          && current.getOperands().get(1) instanceof RexLiteral divisorLit
          && current.getOperands().get(0) instanceof RexCall divChild) {
        divisors.add(divisorLit.getValueAs(BigDecimal.class));
        current = divChild;
        continue;
      }

      break;
    }

    var minusDate = extractMinusDateFromReinterpret(current);
    if (minusDate == null) {
      return call;
    }

    // Bare Reinterpret (no /INT) — unit is fixed by the interval qualifier (e.g. MONTH).
    if (divisors.isEmpty()) {
      var unit =
          DuckDbSqlTranslationUtils.extractTimeUnit(
              minusDate.getType().getIntervalQualifier().getStartUnit());
      return buildDateDiff(unit, minusDate, relBuilder, call);
    }

    // Reinterpret of a day-time interval yields ms; year-month yields months.
    var unit = minusDate.getType().getIntervalQualifier().isYearMonth() ? "month" : "millisecond";
    for (var i = divisors.size() - 1; i >= 0; i--) {
      var next = DuckDbSqlTranslationUtils.divisorToTimeUnit(unit, divisors.get(i));
      if (next == null) {
        return call;
      }
      unit = next;
    }
    return buildDateDiff(unit, minusDate, relBuilder, call);
  }

  /**
   * Collapses {@code /INT(date_diff(innerUnit, a, b), N)} into {@code date_diff(outerUnit, a, b)}
   * when {@code innerUnit * N} is a unit DuckDB knows natively (WEEK, QUARTER).
   */
  private static RexNode rewriteDivide(RelBuilder relBuilder, RexCall call) {
    if (call.getOperands().size() != 2
        || !(call.getOperands().get(0) instanceof RexCall innerCall)
        || !(call.getOperands().get(1) instanceof RexLiteral divisorLit)
        || innerCall.getOperator() != DATE_DIFF
        || innerCall.getOperands().size() != 3
        || !(innerCall.getOperands().get(0) instanceof RexLiteral innerUnitLit)) {
      return call;
    }

    var outerUnit =
        DuckDbSqlTranslationUtils.divisorToTimeUnit(
            innerUnitLit.getValueAs(String.class), divisorLit.getValueAs(BigDecimal.class));
    if (outerUnit == null) {
      return call;
    }

    var rexBuilder = relBuilder.getRexBuilder();
    return rexBuilder.makeCall(
        call.getType(),
        DATE_DIFF,
        List.of(
            rexBuilder.makeLiteral(outerUnit),
            innerCall.getOperands().get(1),
            innerCall.getOperands().get(2)));
  }

  /** Extracts MINUS_DATE from {@code Reinterpret(MINUS_DATE(ts2, ts1))}. */
  private static RexCall extractMinusDateFromReinterpret(RexNode node) {
    if (!(node instanceof RexCall reinterpret)
        || reinterpret.getKind() != SqlKind.REINTERPRET
        || reinterpret.getOperands().isEmpty()) {
      return null;
    }
    if (reinterpret.getOperands().get(0) instanceof RexCall minusDate
        && minusDate.getOperator().getKind() == SqlKind.MINUS
        && minusDate.getOperands().size() == 2) {
      return minusDate;
    }
    return null;
  }

  /**
   * Builds a {@code date_diff(unit, start, end)} RexCall. MINUS_DATE has operands (ts2, ts1)
   * because TIMESTAMPDIFF computes end - start.
   */
  private static RexCall buildDateDiff(
      String unit, RexCall minusDate, RelBuilder relBuilder, RexCall originalCall) {
    var end = minusDate.getOperands().get(0);
    var start = minusDate.getOperands().get(1);
    var rexBuilder = relBuilder.getRexBuilder();
    return (RexCall)
        rexBuilder.makeCall(
            originalCall.getType(), DATE_DIFF, List.of(rexBuilder.makeLiteral(unit), start, end));
  }
}
