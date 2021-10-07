package ai.dataeng.sqml.analyzer;

import static com.google.common.collect.Multimaps.forMap;
import static com.google.common.collect.Multimaps.unmodifiableMultimap;

import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.logical.LogicalPlan;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.BooleanType;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupingOperation;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Script;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class Analysis {
  private final Script script;
  private Map<Expression, Type> typeMap = new HashMap<>();
  private final Map<Node, Scope> scopes = new LinkedHashMap<>();
  private Map<Node, List<Expression>> outputExpressions = new HashMap<>();
  private Map<Relation, RelationType> relations = new HashMap<>();
  private RelationType model;
  private Map<QualifiedName, Field> sourceScopedFields = new HashMap<>();
  private final Map<NodeRef<Expression>, Type> coercions = new LinkedHashMap<>();
  private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
  private final List<Expression> parameters = List.of();
  private final Multimap<NodeRef<Expression>, FieldId> columnReferences = ArrayListMultimap.create();
  private final LogicalPlan logicalPlan = new LogicalPlan();
  private List<SqmlFunction> functions = new ArrayList<>();

  public Analysis(Script script) {
    this.script = script;
  }

  public Optional<Type> getType(Expression expression) {
    Type type = typeMap.get(expression);
    return Optional.ofNullable(type);
  }

  public void setScope(Node node, Scope scope) {
    scopes.put(node, scope);
  }

  public void setOutputExpressions(Node node, List<Expression> expressions) {
    outputExpressions.put(node, expressions);
  }

  public void addTypes(Map<Expression, Type> expressionTypes) {
    this.typeMap.putAll(expressionTypes);
  }

  public void setGroupByExpressions(QuerySpecification node, List<Expression> groupingExpressions) {

  }

  public void setGroupingSets(QuerySpecification node, List<List<Set<FieldId>>> sets) {

  }

  public void setJoinCriteria(Join node, Expression expression) {

  }

  public Optional<RelationType> getRelation(Relation node) {
    return Optional.ofNullable(this.relations.get(node));
  }

  public void addRelations(Map<Relation, RelationType> relations) {
    this.relations.putAll(relations);
  }

  public void setOrderByExpressions(Node node, List<Expression> orderByExpressions) {

  }

  public void recordSubqueries(Node node, ExpressionAnalysis expressionAnalysis) {

  }

  public void addCoercion(Expression predicate, BooleanType instance, boolean b) {

  }

  public void setWhere(Node node, Expression predicate) {

  }

  public void setOrderByAggregates(OrderBy node, List<Expression> orderByAggregationExpressions) {

  }

  public boolean isAggregation(Node node) {
    return false;//return groupByExpressions.containsKey(NodeRef.of(node));
  }

  public void setGroupingOperations(QuerySpecification node,
      List<GroupingOperation> groupingOperations) {

  }

  public void setAggregates(Node node, List<FunctionCall> aggregates) {

  }

  public void setHaving(Node node, Expression predicate) {

  }

  public void setMultiplicity(Node node, Long multiplicity) {

  }

  public void addSourceScopedFields(Map<QualifiedName, Field> sourceScopedFields) {
    this.sourceScopedFields.putAll(sourceScopedFields);
  }

  public Type getCoercion(Expression expression)
  {
    return coercions.get(NodeRef.of(expression));
  }

  public boolean isTypeOnlyCoercion(Expression expression)
  {
    return typeOnlyCoercions.contains(NodeRef.of(expression));
  }
  public void addCoercion(Expression expression, Type type, boolean isTypeOnlyCoercion)
  {
    this.coercions.put(NodeRef.of(expression), type);
    if (isTypeOnlyCoercion) {
      this.typeOnlyCoercions.add(NodeRef.of(expression));
    }
  }
  public void addCoercions(Map<NodeRef<Expression>, Type> coercions, Set<NodeRef<Expression>> typeOnlyCoercions)
  {
    this.coercions.putAll(coercions);
    this.typeOnlyCoercions.addAll(typeOnlyCoercions);
  }
  public List<Expression> getParameters()
  {
    return parameters;
  }


  public Multimap<NodeRef<Expression>, FieldId> getColumnReferenceFields() {
    return unmodifiableMultimap(columnReferences);
  }
  public void addColumnReferences(Map<NodeRef<Expression>, FieldId> columnReferences)
  {
    this.columnReferences.putAll(forMap(columnReferences));
  }

  public void addColumnReference(NodeRef<Expression> node, FieldId fieldId)
  {
    this.columnReferences.put(node, fieldId);
  }

  public LogicalPlan getLogicalPlan() {
    return logicalPlan;
  }

  public List<SqmlFunction> getDefinedFunctions() {
    return functions;
  }

  public void addFunction(SqmlFunction function) {
    this.functions.add(function);
  }
}
