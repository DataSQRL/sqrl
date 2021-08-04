package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.analyzer.FieldId;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.type.SqmlType;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class ExpressionAnalysis {
  Map<Expression, SqmlType> typeMap = new HashMap<>();
  private final Map<NodeRef<Expression>, FieldId> columnReferences = new LinkedHashMap<>();

  public SqmlType getType(Expression node) {
    return typeMap.get(node);
  }

  public void addType(Expression node, SqmlType type) {
    typeMap.put(node, type);
  }

  public Map<Expression, SqmlType> getExpressionTypes() {
    return typeMap;
  }

  public Map<NodeRef<Expression>, FieldId> getColumnReferences() {
    return columnReferences;
  }
}
