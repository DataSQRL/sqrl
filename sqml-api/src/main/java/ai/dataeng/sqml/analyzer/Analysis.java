package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupingOperation;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Script;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.BooleanSqmlType;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Analysis {
  private final Script script;
  private Map<Expression, SqmlType> typeMap = new HashMap<>();
  private final Map<Node, Scope> scopes = new LinkedHashMap<>();
  private Map<Node, List<Expression>> outputExpressions = new HashMap<>();
  private Map<Expression, String> nameMap = new HashMap<>();
  private Map<Relation, RelationSqmlType> relations = new HashMap<>();

  public Analysis(Script script) {
    this.script = script;
  }

  public Optional<SqmlType> getType(Expression expression) {
    SqmlType type = typeMap.get(expression);
    return Optional.ofNullable(type);
  }

  public QualifiedName getName(QualifiedName name) {
    return name;
  }

  public Optional<Boolean> isNonNull(Expression expression) {
    return Optional.empty();
  }

  public void setScope(Node node, Scope scope) {
    scopes.put(node, scope);
  }

  public void setOutputExpressions(Node node, List<Expression> expressions) {
    outputExpressions.put(node, expressions);
  }

  public void addTypes(Map<Expression, SqmlType> expressionTypes) {
    this.typeMap.putAll(expressionTypes);
  }

  public void setGroupByExpressions(QuerySpecification node, List<Expression> groupingExpressions) {

  }

  public void setGroupingSets(QuerySpecification node, List<List<Set<Object>>> sets) {

  }

  public List<Expression> getOutputExpressions(Node node) {
    return outputExpressions.get(node);
  }

  public String getName(Expression expression) {
    return nameMap.get(expression);

  }

  public void setJoinCriteria(Join node, Expression expression) {

  }

  public Optional<RelationSqmlType> getRelation(Relation node) {
    return Optional.ofNullable(this.relations.get(node));
  }

  public void addRelations(Map<Relation, RelationSqmlType> relations) {
    this.relations.putAll(relations);
  }

  public void setOrderByExpressions(Node node, List<Expression> orderByExpressions) {

  }

  public void recordSubqueries(Node node, ExpressionAnalysis expressionAnalysis) {

  }

  public void addCoercion(Expression predicate, BooleanSqmlType instance, boolean b) {

  }

  public void setWhere(Node node, Expression predicate) {

  }

  public void setOrderByAggregates(OrderBy node, List<Expression> orderByAggregationExpressions) {

  }

  public boolean isAggregation(QuerySpecification node) {
    return false;//return groupByExpressions.containsKey(NodeRef.of(node));
  }

  public void setGroupingOperations(QuerySpecification node,
      List<GroupingOperation> groupingOperations) {

  }

  public void setAggregates(Node node, List<FunctionCall> aggregates) {

  }

  public void setHaving(Node node, Expression predicate) {

  }

  public void addName(Expression expression, String name) {
    this.nameMap.put(expression, name);
  }
}
