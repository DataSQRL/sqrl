package com.datasqrl.plan.table;

import com.datasqrl.plan.util.IndexMap;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;

@Getter
public class AddedColumn {

  @Setter String nameId;
  final RexNode expression;

  public AddedColumn(@NonNull String nameId, @NonNull RexNode expression) {
    this.nameId = nameId;
    this.expression = expression;
  }

  public RelDataType getDataType() {
    return expression.getType();
  }

  public RexNode getExpression(IndexMap indexMap, @NonNull RelDataType type) {
    return indexMap.map(expression, type);
  }

  public RexNode getBaseExpression() {
    return expression;
  }

  public int appendTo(@NonNull RelBuilder relBuilder, int atIndex, @NonNull IndexMap indexMap) {
    RelDataType baseType = relBuilder.peek().getRowType();
    int noBaseFields = baseType.getFieldCount();
    List<String> fieldNames = new ArrayList<>(noBaseFields + 1);
    List<RexNode> rexNodes = new ArrayList<>(noBaseFields + 1);
    for (int i = 0; i < noBaseFields; i++) {
      fieldNames.add(i, null); // Calcite will infer name
      rexNodes.add(i, RexInputRef.of(i, baseType));
    }
    fieldNames.add(atIndex, nameId);
    rexNodes.add(atIndex, indexMap.map(expression, relBuilder.peek().getRowType()));
    Preconditions.checkArgument(fieldNames.size() + rexNodes.size() == 2 * (noBaseFields + 1));
    relBuilder.project(rexNodes, fieldNames);
    return noBaseFields;
  }
}
