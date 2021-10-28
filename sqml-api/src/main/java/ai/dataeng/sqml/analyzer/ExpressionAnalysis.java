package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Relation;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ExpressionAnalysis {
  Map<Expression, Type> typeMap = new HashMap<>();
  Map<Relation, RelationType> relations = new HashMap<>();
  Map<QualifiedName, Field> sourceScopedFields = new HashMap<>();
  private final Map<NodeRef<Expression>, Type> expressionCoercions = new LinkedHashMap<>();
  private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
  private final Map<NodeRef<Expression>, FieldId> columnReferences = new LinkedHashMap<>();
  private Map<FunctionCall, SqmlFunction> functionMap = new HashMap<>();

  public Type getType(Expression node) {
    return typeMap.get(node);
  }

  public void addType(Expression node, Type type) {
    typeMap.put(node, type);
  }

  public Map<Expression, Type> getExpressionTypes() {
    return typeMap;
  }

  public Optional<RelationType> getRelation(Relation node) {
    return Optional.ofNullable(this.relations.get(node));
  }

  public void setRelation(Relation node, RelationType type) {
    this.relations.put(node, type);
  }

  public void addSourceScopedType(QualifiedName path, Field field) {
    sourceScopedFields.put(path, field);
  }

  public Map<QualifiedName, Field> getSourceScopedFields() {
    return sourceScopedFields;
  }

  public Map<NodeRef<Expression>, Type> getExpressionCoercions() {
    return expressionCoercions;
  }

  public Set<NodeRef<Expression>> getTypeOnlyCoercions() {
    return typeOnlyCoercions;
  }

  public Map<NodeRef<Expression>, FieldId> getColumnReferences() {
    return columnReferences;
  }

  public void qualifyFunction(FunctionCall node, SqmlFunction function) {
    this.functionMap.put(node, function);
  }

  public Map<FunctionCall, SqmlFunction> getFunctionMap() {
    return functionMap;
  }

  public SqmlFunction getFunction(FunctionCall node) {
    return functionMap.get(node);
  }
}
