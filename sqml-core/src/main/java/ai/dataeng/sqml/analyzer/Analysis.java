/*
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
package ai.dataeng.sqml.analyzer;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Multimaps.forMap;
import static java.lang.String.format;
import static java.util.Collections.emptyList;
import static java.util.Collections.unmodifiableCollection;
import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.Model;
import ai.dataeng.sqml.StubModel;
import ai.dataeng.sqml.common.ColumnHandle;
import ai.dataeng.sqml.common.QualifiedObjectName;
import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.function.FunctionHandle;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.planner.AssignmentContext;
import ai.dataeng.sqml.relation.TableHandle;
import ai.dataeng.sqml.sql.tree.ExistsPredicate;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.FunctionCall;
import ai.dataeng.sqml.sql.tree.GroupingOperation;
import ai.dataeng.sqml.sql.tree.InPredicate;
import ai.dataeng.sqml.sql.tree.Join;
import ai.dataeng.sqml.sql.tree.Node;
import ai.dataeng.sqml.sql.tree.NodeRef;
import ai.dataeng.sqml.sql.tree.OrderBy;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import ai.dataeng.sqml.sql.tree.QuantifiedComparisonExpression;
import ai.dataeng.sqml.sql.tree.QuerySpecification;
import ai.dataeng.sqml.sql.tree.Relation;
import ai.dataeng.sqml.sql.tree.Statement;
import ai.dataeng.sqml.sql.tree.SubqueryExpression;
import ai.dataeng.sqml.sql.tree.Table;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class Analysis {
  //Statements
  private List<Expression> parameters;
  private final Map<NodeRef<Node>, Scope> scopes = new LinkedHashMap<>();
  private final Multimap<NodeRef<Expression>, FieldId> columnReferences = ArrayListMultimap
      .create();
  // a map of users to the columns per table that they access
  private final Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> tableColumnReferences = new LinkedHashMap<>();
  private final Map<NodeRef<QuerySpecification>, List<FunctionCall>> aggregates = new LinkedHashMap<>();
  private final Map<NodeRef<OrderBy>, List<Expression>> orderByAggregates = new LinkedHashMap<>();
  private final Map<NodeRef<QuerySpecification>, List<Expression>> groupByExpressions = new LinkedHashMap<>();
  private final Map<NodeRef<QuerySpecification>, GroupingSetAnalysis> groupingSets = new LinkedHashMap<>();
  private final Map<NodeRef<Node>, Expression> where = new LinkedHashMap<>();
  private final Map<NodeRef<QuerySpecification>, Expression> having = new LinkedHashMap<>();
  private final Map<NodeRef<Node>, List<Expression>> orderByExpressions = new LinkedHashMap<>();
  private final Set<NodeRef<OrderBy>> redundantOrderBy = new HashSet<>();
  private final Map<NodeRef<Node>, List<Expression>> outputExpressions = new LinkedHashMap<>();
  private final Map<NodeRef<Join>, Expression> joins = new LinkedHashMap<>();
  private final ListMultimap<NodeRef<Node>, InPredicate> inPredicatesSubqueries = ArrayListMultimap
      .create();
  private final ListMultimap<NodeRef<Node>, SubqueryExpression> scalarSubqueries = ArrayListMultimap
      .create();
  private final ListMultimap<NodeRef<Node>, ExistsPredicate> existsSubqueries = ArrayListMultimap
      .create();
  private final ListMultimap<NodeRef<Node>, QuantifiedComparisonExpression> quantifiedComparisonSubqueries = ArrayListMultimap
      .create();
  private final Map<NodeRef<Table>, TableHandle> tables = new LinkedHashMap<>();
  private final Map<NodeRef<Expression>, Type> types = new LinkedHashMap<>();
  private final Map<NodeRef<Expression>, Type> coercions = new LinkedHashMap<>();
  private final Set<NodeRef<Expression>> typeOnlyCoercions = new LinkedHashSet<>();
  private final Map<NodeRef<Relation>, List<Type>> relationCoercions = new LinkedHashMap<>();
  private final Map<NodeRef<FunctionCall>, FunctionHandle> functionHandles = new LinkedHashMap<>();
  private final Map<Field, ColumnHandle> columns = new LinkedHashMap<>();
  private final Map<NodeRef<QuerySpecification>, List<GroupingOperation>> groupingOperations = new LinkedHashMap<>();
  private Statement statement;
  private final QualifiedName name;

  public Analysis() {
    this.name = null;
  }

  //Contextual analysis
  public Analysis(Statement statement, List<Expression> parameters,
      QualifiedName name) {
    this.statement = statement;
    this.parameters = parameters;
    this.name = name;
  }

  public void setAggregates(QuerySpecification node, List<FunctionCall> aggregates) {
    this.aggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
  }

  public List<FunctionCall> getAggregates(QuerySpecification query) {
    return aggregates.get(NodeRef.of(query));
  }

  public void setOrderByAggregates(OrderBy node, List<Expression> aggregates) {
    this.orderByAggregates.put(NodeRef.of(node), ImmutableList.copyOf(aggregates));
  }

  public List<Expression> getOrderByAggregates(OrderBy node) {
    return orderByAggregates.get(NodeRef.of(node));
  }

  public Map<NodeRef<Expression>, Type> getTypes() {
    return unmodifiableMap(types);
  }

  public Type getType(Expression expression) {
    Type type = types.get(NodeRef.of(expression));
    checkArgument(type != null, "Expression not analyzed: %s", expression);
    return type;
  }

  public Type getTypeWithCoercions(Expression expression) {
    NodeRef<Expression> key = NodeRef.of(expression);
    checkArgument(types.containsKey(key), "Expression not analyzed: %s", expression);
    if (coercions.containsKey(key)) {
      return coercions.get(key);
    }
    return types.get(key);
  }

  public Type[] getRelationCoercion(Relation relation) {
    return Optional.ofNullable(relationCoercions.get(NodeRef.of(relation)))
        .map(types -> types.stream().toArray(Type[]::new))
        .orElse(null);
  }

  public void addRelationCoercion(Relation relation, Type[] types) {
    relationCoercions.put(NodeRef.of(relation), ImmutableList.copyOf(types));
  }

  public Map<NodeRef<Expression>, Type> getCoercions() {
    return unmodifiableMap(coercions);
  }

  public Type getCoercion(Expression expression) {
    return coercions.get(NodeRef.of(expression));
  }

  public void setGroupingSets(QuerySpecification node, GroupingSetAnalysis groupingSets) {
    this.groupingSets.put(NodeRef.of(node), groupingSets);
  }

  public void setGroupByExpressions(QuerySpecification node, List<Expression> expressions) {
    groupByExpressions.put(NodeRef.of(node), expressions);
  }

  public boolean isAggregation(QuerySpecification node) {
    return groupByExpressions.containsKey(NodeRef.of(node));
  }

  public boolean isTypeOnlyCoercion(Expression expression) {
    return typeOnlyCoercions.contains(NodeRef.of(expression));
  }

  public GroupingSetAnalysis getGroupingSets(QuerySpecification node) {
    return groupingSets.get(NodeRef.of(node));
  }

  public List<Expression> getGroupByExpressions(QuerySpecification node) {
    return groupByExpressions.get(NodeRef.of(node));
  }

  public void setWhere(Node node, Expression expression) {
    where.put(NodeRef.of(node), expression);
  }

  public Expression getWhere(QuerySpecification node) {
    return where.get(NodeRef.<Node>of(node));
  }

  public void setOrderByExpressions(Node node, List<Expression> items) {
    orderByExpressions.put(NodeRef.of(node), ImmutableList.copyOf(items));
  }

  public List<Expression> getOrderByExpressions(Node node) {
    return orderByExpressions.get(NodeRef.of(node));
  }

  public void setOutputExpressions(Node node, List<Expression> expressions) {
    outputExpressions.put(NodeRef.of(node), ImmutableList.copyOf(expressions));
  }

  public List<Expression> getOutputExpressions(Node node) {
    return outputExpressions.get(NodeRef.of(node));
  }

  public void setHaving(QuerySpecification node, Expression expression) {
    having.put(NodeRef.of(node), expression);
  }

  public void setJoinCriteria(Join node, Expression criteria) {
    joins.put(NodeRef.of(node), criteria);
  }

  public Expression getJoinCriteria(Join join) {
    return joins.get(NodeRef.of(join));
  }

  public void recordSubqueries(Node node, ExpressionAnalysis expressionAnalysis) {
    NodeRef<Node> key = NodeRef.of(node);
    this.inPredicatesSubqueries
        .putAll(key, dereference(expressionAnalysis.getSubqueryInPredicates()));
    this.scalarSubqueries.putAll(key, dereference(expressionAnalysis.getScalarSubqueries()));
    this.existsSubqueries.putAll(key, dereference(expressionAnalysis.getExistsSubqueries()));
    this.quantifiedComparisonSubqueries
        .putAll(key, dereference(expressionAnalysis.getQuantifiedComparisons()));
  }

  private <T extends Node> List<T> dereference(Collection<NodeRef<T>> nodeRefs) {
    return nodeRefs.stream()
        .map(NodeRef::getNode)
        .collect(toImmutableList());
  }

  public List<InPredicate> getInPredicateSubqueries(Node node) {
    return ImmutableList.copyOf(inPredicatesSubqueries.get(NodeRef.of(node)));
  }

  public List<SubqueryExpression> getScalarSubqueries(Node node) {
    return ImmutableList.copyOf(scalarSubqueries.get(NodeRef.of(node)));
  }

  public boolean isScalarSubquery(SubqueryExpression subqueryExpression) {
    return scalarSubqueries.values().contains(subqueryExpression);
  }

  public List<ExistsPredicate> getExistsSubqueries(Node node) {
    return ImmutableList.copyOf(existsSubqueries.get(NodeRef.of(node)));
  }

  public List<QuantifiedComparisonExpression> getQuantifiedComparisonSubqueries(Node node) {
    return unmodifiableList(quantifiedComparisonSubqueries.get(NodeRef.of(node)));
  }

  public void addColumnReferences(Map<NodeRef<Expression>, FieldId> columnReferences) {
    this.columnReferences.putAll(forMap(columnReferences));
  }

  public void addColumnReference(NodeRef<Expression> node, FieldId fieldId) {
    this.columnReferences.put(node, fieldId);
  }

  //Todo: Add the extra scoping modifier
  public Scope getScope(Node node,
      AssignmentContext context) {
    Optional<Scope> scope = tryGetScope(node);
    if (scope.isEmpty()) {
      scope = tryGetModelScope(node, context);
    }

    return scope.orElseThrow(() -> new IllegalArgumentException(
        format("Analysis does not contain information for node: %s", node)));
  }

  private Optional<Scope> tryGetModelScope(Node node,
      AssignmentContext context) {
    //todo Query scope
      Scope queryScope = Scope.builder()
          .build();
      return Optional.of(queryScope);
  }

  public Optional<Scope> tryGetScope(Node node) {
    NodeRef<Node> key = NodeRef.of(node);
    if (scopes.containsKey(key)) {
      return Optional.of(scopes.get(key));
    }

    return Optional.empty();
  }

  public void setScope(Node node, Scope scope) {
    scopes.put(NodeRef.of(node), scope);
  }

  public RelationType getOutputDescriptor(Node node) {
    return getScope(node, null).getRelationType();
  }

  public TableHandle getTableHandle(Table table) {
    TableHandle tableHandle = tables.get(NodeRef.of(table));
    if (tableHandle == null) {
      return new TableHandle();
    }
    return tableHandle;
  }

  public Collection<TableHandle> getTables() {
    return unmodifiableCollection(tables.values());
  }

  public void registerTable(Table table, TableHandle handle) {
    tables.put(NodeRef.of(table), handle);
  }

  public FunctionHandle getFunctionHandle(FunctionCall function) {
    return functionHandles.get(NodeRef.of(function));
  }

  public Map<NodeRef<FunctionCall>, FunctionHandle> getFunctionHandles() {
    return ImmutableMap.copyOf(functionHandles);
  }

  public void addFunctionHandles(Map<NodeRef<FunctionCall>, FunctionHandle> infos) {
    functionHandles.putAll(infos);
  }

  public Set<NodeRef<Expression>> getColumnReferences() {
    return unmodifiableSet(columnReferences.keySet());
  }

  public Multimap<NodeRef<Expression>, FieldId> getColumnReferenceFields() {
    return ImmutableListMultimap.copyOf(columnReferences);
  }

  public boolean isColumnReference(Expression expression) {
    requireNonNull(expression, "expression is null");
    checkArgument(getType(expression) != null, "expression %s has not been analyzed", expression);
    return columnReferences.containsKey(NodeRef.of(expression));
  }

  public void addTypes(Map<NodeRef<Expression>, Type> types) {
    this.types.putAll(types);
  }

  public void addCoercion(Expression expression, Type type, boolean isTypeOnlyCoercion) {
    this.coercions.put(NodeRef.of(expression), type);
    if (isTypeOnlyCoercion) {
      this.typeOnlyCoercions.add(NodeRef.of(expression));
    }
  }

  public void addCoercions(Map<NodeRef<Expression>, Type> coercions,
      Set<NodeRef<Expression>> typeOnlyCoercions) {
    this.coercions.putAll(coercions);
    this.typeOnlyCoercions.addAll(typeOnlyCoercions);
  }

  public Expression getHaving(QuerySpecification query) {
    return having.get(NodeRef.of(query));
  }

  public void setColumn(Field field, ColumnHandle handle) {
    columns.put(field, handle);
  }

  public ColumnHandle getColumn(Field field) {
    return columns.get(field);
  }

  public void setGroupingOperations(QuerySpecification querySpecification,
      List<GroupingOperation> groupingOperations) {
    this.groupingOperations
        .put(NodeRef.of(querySpecification), ImmutableList.copyOf(groupingOperations));
  }

  public List<GroupingOperation> getGroupingOperations(QuerySpecification querySpecification) {
    return Optional.ofNullable(groupingOperations.get(NodeRef.of(querySpecification)))
        .orElse(emptyList());
  }

  public List<Expression> getParameters() {
    return parameters;
  }

  public void addTableColumnReferences(Multimap<QualifiedObjectName, String> tableColumnMap) {
    AccessControlInfo accessControlInfo = new AccessControlInfo();
    Map<QualifiedObjectName, Set<String>> references = tableColumnReferences
        .computeIfAbsent(accessControlInfo, k -> new LinkedHashMap<>());
    tableColumnMap.asMap()
        .forEach(
            (key, value) -> references.computeIfAbsent(key, k -> new HashSet<>()).addAll(value));
  }

  public Map<AccessControlInfo, Map<QualifiedObjectName, Set<String>>> getTableColumnReferences() {
    return tableColumnReferences;
  }

  public void markRedundantOrderBy(OrderBy orderBy) {
    redundantOrderBy.add(NodeRef.of(orderBy));
  }

  public boolean isOrderByRedundant(OrderBy orderBy) {
    return redundantOrderBy.contains(NodeRef.of(orderBy));
  }

  public StubModel createStubModel(Metadata metadata) {
    return null;
  }

  public static class GroupingSetAnalysis {

    private final List<Set<FieldId>> cubes;
    private final List<List<FieldId>> rollups;
    private final List<List<Set<FieldId>>> ordinarySets;
    private final List<Expression> complexExpressions;

    public GroupingSetAnalysis(
        List<Set<FieldId>> cubes,
        List<List<FieldId>> rollups,
        List<List<Set<FieldId>>> ordinarySets,
        List<Expression> complexExpressions) {
      this.cubes = ImmutableList.copyOf(cubes);
      this.rollups = ImmutableList.copyOf(rollups);
      this.ordinarySets = ImmutableList.copyOf(ordinarySets);
      this.complexExpressions = ImmutableList.copyOf(complexExpressions);
    }

    public List<Set<FieldId>> getCubes() {
      return cubes;
    }

    public List<List<FieldId>> getRollups() {
      return rollups;
    }

    public List<List<Set<FieldId>>> getOrdinarySets() {
      return ordinarySets;
    }

    public List<Expression> getComplexExpressions() {
      return complexExpressions;
    }
  }

  public static final class AccessControlInfo {

    public AccessControlInfo() {
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      AccessControlInfo that = (AccessControlInfo) o;
      return true;
    }

    @Override
    public int hashCode() {
      return Objects.hash();
    }

    @Override
    public String toString() {
      return format("AccessControl:");
    }
  }
}
