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

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform;
import com.datasqrl.calcite.dialect.DuckDbSqlDialect;
import com.datasqrl.calcite.function.OperatorRuleTransform;
import com.google.auto.service.AutoService;
import java.util.List;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;

/**
 * DuckDB has a TRY_CAST function, this is a workaround, cause currently Flink's
 * SqlNodeConvertContext#toRelRoot(...) swallows the target type, and we cannot compile it
 * otherwise.
 */
@AutoService(OperatorRuleTransform.class)
public class TryCastSqlTranslation implements OperatorRuleTransform {

  private static SqlOperator createTryCastOperator(SqlNode targetTypeSpec) {
    return new SqlSpecialOperator("TRY_CAST", SqlKind.OTHER_FUNCTION) {
      @Override
      public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        // DuckDB syntax: TRY_CAST(value AS <type>)
        writer.keyword("TRY_CAST");
        writer.print("(");
        call.operand(0).unparse(writer, 0, 0);
        writer.print(" AS ");
        targetTypeSpec.unparse(writer, 0, 0);
        writer.print(")");
      }
    };
  }

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
                      var rexBuilder = relBuilder.getRexBuilder();

                      // TRY_CAST returns NULL on failure, so the return type must be nullable.
                      var returnType =
                          rexBuilder.getTypeFactory().createTypeWithNullability(targetType, true);

                      // Bake the dialect-specific type spec into the operator.
                      var targetTypeSpec = DuckDbSqlDialect.DEFAULT.getCastSpec(targetType);
                      var tryCast = createTryCastOperator(targetTypeSpec);

                      // Emit: TRY_CAST(value AS <targetType>)
                      return rexBuilder.makeCall(returnType, tryCast, List.of(value));
                    })
                .toRule());
  }

  @Override
  public Dialect getDialect() {
    return Dialect.DUCKDB;
  }

  @Override
  public String getRuleOperatorName() {
    return "try_cast";
  }
}
