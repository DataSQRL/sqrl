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

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SimpleCallTransform.SimpleCallTransformConfig;
import com.datasqrl.calcite.function.OperatorRuleTransform;
import com.datasqrl.flinkrunner.stdlib.json.JsonFunctions;
import com.google.auto.service.AutoService;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

/**
 * Serializes typed ROW values as JSONB objects for PostgreSQL.
 *
 * <p>PostgreSQL does not support casting a record directly to JSONB. Keeping the ROW type in the
 * relational plan preserves nested-field metadata for the API, while the existing JSON_OBJECT SQL
 * translation renders this call as {@code jsonb_build_object(...)} for PostgreSQL.
 */
@AutoService(OperatorRuleTransform.class)
public class PostgresRecordCastTranslation implements OperatorRuleTransform {

  private static final SqlOperator JSON_OBJECT = lightweightOp(JsonFunctions.JSON_OBJECT);

  @Override
  public List<RelRule> transform(SqlOperator operator) {
    return List.of(
        (RelRule)
            SimpleCallTransformConfig.createConfig(
                    operator,
                    (relBuilder, call) -> {
                      var callOps = call.getOperands();
                      if (call.getKind() != SqlKind.CAST || callOps.size() != 1) {
                        return call;
                      }

                      if (!(callOps.get(0) instanceof RexCall row)
                          || row.getKind() != SqlKind.ROW
                          || !call.getType().isStruct()
                          || row.getOperands().size() != call.getType().getFieldCount()) {
                        return call;
                      }

                      var arguments = new ArrayList<RexNode>();
                      var rexBuilder = relBuilder.getRexBuilder();
                      var fieldNames = call.getType().getFieldNames();
                      for (var i = 0; i < fieldNames.size(); i++) {
                        arguments.add(rexBuilder.makeLiteral(fieldNames.get(i)));
                        arguments.add(row.getOperands().get(i));
                      }

                      return rexBuilder.makeCall(call.getType(), JSON_OBJECT, arguments);
                    })
                .toRule());
  }

  @Override
  public Dialect getDialect() {
    return Dialect.POSTGRES;
  }

  @Override
  public String getRuleOperatorName() {
    return "cast";
  }
}
