package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

public class ExpressionAnalysis {
  Map<Expression, SqmlType> typeMap = new HashMap<>();
  Map<Relation, RelationSqmlType> relations = new HashMap<>();

  public SqmlType getType(Expression node) {
    return typeMap.get(node);
  }

  public void addType(Expression node, SqmlType type) {
    typeMap.put(node, type);
  }

  public Map<Expression, SqmlType> getExpressionTypes() {
    return typeMap;
  }

  public Optional<RelationSqmlType> getRelation(Relation node) {
    return Optional.ofNullable(this.relations.get(node));
  }

  public void setRelation(Relation node, RelationSqmlType type) {
    this.relations.put(node, type);
  }
}
