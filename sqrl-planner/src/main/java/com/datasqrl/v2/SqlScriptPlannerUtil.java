package com.datasqrl.v2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.tools.RelBuilder;

import com.datasqrl.v2.tables.SqrlFunctionParameter;
import com.google.common.base.Preconditions;

public class SqlScriptPlannerUtil {

  public static List<FunctionParameter> addFilterByColumn(RelBuilder relB, List<Integer> columnIndexes, boolean optional) {
    return addFilterByColumn(relB, columnIndexes, optional, 0);
  }

  public static List<FunctionParameter> addFilterByColumn(RelBuilder relB, List<Integer> columnIndexes, boolean optional, int paramOffset) {
    Preconditions.checkArgument(!columnIndexes.isEmpty());
    var fields = relB.peek().getRowType().getFieldList();
    Preconditions.checkArgument(columnIndexes.stream().allMatch(i -> i < fields.size()),"Invalid column indexes: %s", columnIndexes);
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
        condition = relB.or(condition, relB.and(relB.isNull(param),relB.isNull(relB.field(colIndex))));
      }
      conditions.add(condition);
      parameters.add(new SqrlFunctionParameter(field.getName(), ordinal, paramType,false));
    }
    relB.filter(relB.and(conditions));
    return parameters;
  }

}
