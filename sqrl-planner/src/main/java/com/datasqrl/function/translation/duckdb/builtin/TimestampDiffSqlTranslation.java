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
import java.util.List;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
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
 */
@AutoService(OperatorRuleTransform.class)
public class TimestampDiffSqlTranslation implements OperatorRuleTransform {

  @Override
  public List<RelRule> transform(SqlOperator operator) {
    return List.of(
        // The outermost node is always CAST — Calcite produces either:
        //   CAST(DIVIDE_INTEGER(Reinterpret(MINUS_DATE(ts2, ts1)), divisor)) for most units
        //   CAST(Reinterpret(MINUS_DATE(ts2, ts1))) when multiplier == divisor (SECOND, MONTH)
        (RelRule)
            SimpleCallTransform.SimpleCallTransformConfig.createConfig(
                    SqlStdOperatorTable.CAST, TimestampDiffSqlTranslation::rewriteCast)
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
   * Handles two patterns:
   *
   * <ul>
   *   <li>{@code CAST(/INT(Reinterpret(MINUS_DATE(ts2, ts1)), divisor))} — most units
   *   <li>{@code CAST(Reinterpret(MINUS_DATE(ts2, ts1)))} — SECOND, MONTH (no division)
   * </ul>
   */
  private static RexNode rewriteCast(RelBuilder relBuilder, RexCall call) {
    if (call.getOperands().isEmpty()) {
      return call;
    }
    var inner = call.getOperands().get(0);
    if (!(inner instanceof RexCall innerCall)) {
      return call;
    }

    // Unwrap nested CASTs — Calcite may produce CAST(CAST(/INT(Reinterpret(...), d)))
    while (innerCall.getKind() == SqlKind.CAST
        && !innerCall.getOperands().isEmpty()
        && innerCall.getOperands().get(0) instanceof RexCall nested) {
      innerCall = nested;
    }

    // Pattern 1: CAST(/INT(Reinterpret(MINUS_DATE(...)), divisor))
    if (innerCall.getKind() == SqlKind.DIVIDE
        && innerCall.getOperands().size() == 2
        && innerCall.getOperands().get(1) instanceof RexLiteral divisorLit) {
      var divisorValue = divisorLit.getValueAs(BigDecimal.class);
      var unit = DuckDbSqlTranslationUtils.divisorToTimeUnit(divisorValue);
      if (unit != null) {
        var minusDate = extractMinusDateFromReinterpret(innerCall.getOperands().get(0));
        if (minusDate != null) {
          return buildDateDiff(unit, minusDate, relBuilder, call);
        }
      }
    }

    // Pattern 2: CAST(Reinterpret(MINUS_DATE(...))) — no division
    var minusDate = extractMinusDateFromReinterpret(innerCall);
    if (minusDate != null) {
      var unit =
          DuckDbSqlTranslationUtils.extractTimeUnit(
              minusDate.getType().getIntervalQualifier().getStartUnit());
      return buildDateDiff(unit, minusDate, relBuilder, call);
    }

    return call;
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
    var dateDiffOp = createDateDiffOperator(unit);
    var rexBuilder = relBuilder.getRexBuilder();
    return (RexCall) rexBuilder.makeCall(originalCall.getType(), dateDiffOp, List.of(start, end));
  }

  private static SqlOperator createDateDiffOperator(String unit) {
    return new SqlSpecialOperator(
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
        writer.print("'" + unit + "'");
        writer.sep(",", true);
        call.operand(0).unparse(writer, 0, 0);
        writer.sep(",", true);
        call.operand(1).unparse(writer, 0, 0);
        writer.endFunCall(frame);
      }
    };
  }
}
