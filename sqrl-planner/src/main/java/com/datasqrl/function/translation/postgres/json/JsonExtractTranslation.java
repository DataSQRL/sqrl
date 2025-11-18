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
package com.datasqrl.function.translation.postgres.json;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;
import static com.datasqrl.function.PgSpecificOperatorTable.JsonToString;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform.SimpleCallTransformConfig;
import com.datasqrl.calcite.function.OperatorRuleTransform;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/** Extracts a scalar value based on a JSON path. */
@AutoService(OperatorRuleTransform.class)
public class JsonExtractTranslation implements OperatorRuleTransform {

  public static final SqlFunction PG_JSONB_PATH_QUERY_FIRST =
      lightweightOp("jsonb_path_query_first");

  @Override
  public List<RelRule> transform(SqlOperator operator) {
    return postgresTransform(operator);
  }

  @Override
  public Dialect getDialect() {
    return Dialect.POSTGRES;
  }

  @Override
  public String getRuleOperatorName() {
    return "jsonb_extract";
  }

  private List<RelRule> postgresTransform(SqlOperator operator) {
    return List.of(
        (RelRule)
            SimpleCallTransformConfig.createConfig(
                    operator,
                    (rexBuilder, call) -> {
                      List<RexNode> operands = new ArrayList<>(call.getOperands());
                      if (call.getOperands().size() == 3
                          && call.getOperands().get(2).getType().getSqlTypeName()
                              != SqlTypeName.NULL) {

                        var query =
                            rexBuilder
                                .getRexBuilder()
                                .makeCall(
                                    rexBuilder
                                        .getRexBuilder()
                                        .getTypeFactory()
                                        .createSqlType(SqlTypeName.ANY),
                                    PG_JSONB_PATH_QUERY_FIRST,
                                    operands.subList(0, 2));

                        var type = call.getOperands().get(2).getType();

                        // Strings would otherwise come back as quoted strings unless we cast to
                        // string with the jsonb function
                        RexNode op1ToType;
                        if (SqlTypeName.CHAR_TYPES.contains(type.getSqlTypeName())) {
                          op1ToType =
                              rexBuilder
                                  .getRexBuilder()
                                  .makeCall(
                                      JsonToString,
                                      query,
                                      rexBuilder.getRexBuilder().makeLiteral("{}"));
                        } else {
                          op1ToType = rexBuilder.getRexBuilder().makeCast(type, query, true);
                        }

                        var defaultValue = call.getOperands().get(2);

                        return rexBuilder
                            .getRexBuilder()
                            .makeCall(
                                rexBuilder
                                    .getRexBuilder()
                                    .getTypeFactory()
                                    .createSqlType(SqlTypeName.ANY),
                                SqlStdOperatorTable.COALESCE,
                                List.of(op1ToType, defaultValue));
                      }

                      var query =
                          rexBuilder
                              .getRexBuilder()
                              .makeCall(
                                  rexBuilder
                                      .getRexBuilder()
                                      .getTypeFactory()
                                      .createSqlType(SqlTypeName.ANY),
                                  PG_JSONB_PATH_QUERY_FIRST,
                                  operands.subList(0, 2));
                      return rexBuilder
                          .getRexBuilder()
                          .makeCall(
                              JsonToString, query, rexBuilder.getRexBuilder().makeLiteral("{}"));
                    })
                .toRule());
  }
}
