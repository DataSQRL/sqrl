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
package com.datasqrl.planner;

import com.datasqrl.planner.tables.SqrlFunctionParameter;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.tools.RelBuilder;

public class SqlScriptPlannerUtil {

  public static List<FunctionParameter> addFilterByColumn(
      RelBuilder relB, List<Integer> columnIndexes, boolean optional) {
    return addFilterByColumn(relB, columnIndexes, optional, 0);
  }

  public static List<FunctionParameter> addFilterByColumn(
      RelBuilder relB, List<Integer> columnIndexes, boolean optional, int paramOffset) {
    Preconditions.checkArgument(!columnIndexes.isEmpty());
    var fields = relB.peek().getRowType().getFieldList();
    Preconditions.checkArgument(
        columnIndexes.stream().allMatch(i -> i < fields.size()),
        "Invalid column indexes: %s",
        columnIndexes);
    var paramCounter = new AtomicInteger(paramOffset);
    List<RexNode> conditions = new ArrayList<>();
    List<FunctionParameter> parameters = new ArrayList<>();
    for (Integer colIndex : columnIndexes) {
      var field = fields.get(colIndex);
      var ordinal = paramCounter.getAndIncrement();
      var paramType = relB.getTypeFactory().createTypeWithNullability(field.getType(), optional);
      var param = new RexDynamicParam(paramType, ordinal);
      var condition = relB.equals(relB.field(colIndex), param);
      if (optional) {
        condition = relB.or(condition, relB.isNull(param));
      } else if (field.getType().isNullable()) {
        condition =
            relB.or(condition, relB.and(relB.isNull(param), relB.isNull(relB.field(colIndex))));
      }
      conditions.add(condition);
      parameters.add(new SqrlFunctionParameter(field.getName(), ordinal, paramType));
    }
    relB.filter(relB.and(conditions));
    return parameters;
  }
}
