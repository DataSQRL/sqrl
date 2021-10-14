package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.logical.RelationDefinition;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupingOperation;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Table;
import com.google.common.collect.Multimap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;

@Getter
public class StatementAnalysis {
  Set<RelationDefinition> referencedRelations = new HashSet<>();
  public void setGroupingOperations(QuerySpecification node,
      List<GroupingOperation> groupingOperations) {

  }

  public void setAggregates(QuerySpecification node, List<FunctionCall> aggregates) {

  }

  public boolean isAggregation(QuerySpecification node) {
    return false;
  }

  public Multimap<NodeRef<Expression>, FieldId> getColumnReferenceFields() {
    return null;
  }

  public List<Expression> getParameters() {
    return null;
  }

  public void recordSubqueries(Node node, ExpressionAnalysis expressionAnalysis) {

  }

  public void setWhere(Node node, Expression predicate) {

  }

  public Optional<Type> getType(Expression expression) {
    return null;
  }

  public void setScope(Node node, Scope scope) {

  }

  public void setHaving(QuerySpecification node, Expression predicate) {

  }

  public void setOutputExpressions(QuerySpecification node, List<Expression> outputExpressions) {

  }

  public void addCoercions(Map<NodeRef<Expression>, Type> expressionCoercions,
      Set<NodeRef<Expression>> typeOnlyCoercions) {

  }

  public void addTypes(Map<Expression, Type> expressionTypes) {

  }

  public void addSourceScopedFields(Map<QualifiedName, Field> sourceScopedFields) {

  }

  public void setGroupByExpressions(Node node, List<Expression> groupingExpressions) {

  }

  public void setOrderByExpressions(Node node, List<Expression> orderByExpressions) {

  }

  public void addRelatedRelation(RelationDefinition relationDefinition) {
    referencedRelations.add(relationDefinition);
  }

  public void setMultiplicity(Node node, Optional<Long> parseLimit) {

  }

  public Optional<Long> getMultiplicity(QueryBody queryBody) {
    return Optional.empty();
  }
}
