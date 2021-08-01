package ai.dataeng.sqml.expression;

import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.type.SqmlType;
import java.util.HashMap;
import java.util.Map;

public class ExpressionAnalysis {
  Map<Node, SqmlType> typeMap = new HashMap<>();
  public SqmlType getType(Node node) {
    return typeMap.get(node);
  }

  public void addType(Node node, SqmlType type) {
    typeMap.put(node, type);
  }
}
