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
package ai.dataeng.sqml.planner;

import static ai.dataeng.sqml.analyzer.ExpressionTreeUtils.isEqualComparisonExpression;
import static ai.dataeng.sqml.analyzer.SemanticExceptions.notSupportedException;
import static ai.dataeng.sqml.plan.AggregationNode.singleGroupingSet;
import static ai.dataeng.sqml.plan.ProjectNode.Locality.LOCAL;
import static ai.dataeng.sqml.relational.OriginalExpressionUtils.asSymbolReference;
import static ai.dataeng.sqml.relational.OriginalExpressionUtils.castToRowExpression;
import static ai.dataeng.sqml.sql.tree.Join.Type.INNER;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.Session;
import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.analyzer.Field;
import ai.dataeng.sqml.analyzer.RelationId;
import ai.dataeng.sqml.analyzer.RelationType;
import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.common.ColumnHandle;
import ai.dataeng.sqml.common.ExpressionUtils;
import ai.dataeng.sqml.common.predicate.TupleDomain;
import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.plan.AggregationNode;
import ai.dataeng.sqml.plan.Assignments;
import ai.dataeng.sqml.plan.ExceptNode;
import ai.dataeng.sqml.plan.FilterNode;
import ai.dataeng.sqml.plan.IntersectNode;
import ai.dataeng.sqml.plan.JoinNode;
import ai.dataeng.sqml.plan.PlanNode;
import ai.dataeng.sqml.plan.ProjectNode;
import ai.dataeng.sqml.plan.TableScanNode;
import ai.dataeng.sqml.plan.UnionNode;
import ai.dataeng.sqml.planner.optimizations.JoinNodeUtils;
import ai.dataeng.sqml.relation.TableHandle;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import ai.dataeng.sqml.sql.tree.AliasedRelation;
import ai.dataeng.sqml.sql.tree.Cast;
import ai.dataeng.sqml.sql.tree.ComparisonExpression;
import ai.dataeng.sqml.sql.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.sql.tree.Except;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.InPredicate;
import ai.dataeng.sqml.sql.tree.Intersect;
import ai.dataeng.sqml.sql.tree.IsNotNullPredicate;
import ai.dataeng.sqml.sql.tree.Join;
import ai.dataeng.sqml.sql.tree.QualifiedName;
import ai.dataeng.sqml.sql.tree.Query;
import ai.dataeng.sqml.sql.tree.QuerySpecification;
import ai.dataeng.sqml.sql.tree.Relation;
import ai.dataeng.sqml.sql.tree.SetOperation;
import ai.dataeng.sqml.sql.tree.SymbolReference;
import ai.dataeng.sqml.sql.tree.Table;
import ai.dataeng.sqml.sql.tree.TableSubquery;
import ai.dataeng.sqml.sql.tree.Union;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class RelationPlanner
    extends DefaultTraversalVisitor<RelationPlan, AssignmentContext> {

  private final Analysis analysis;
  private final PlanVariableAllocator variableAllocator;
  private final PlanNodeIdAllocator idAllocator;
  private final Metadata metadata;
  private final Session session;
  private final SubqueryPlanner subqueryPlanner;
  private final AssignmentContext context;

  public RelationPlanner(
      Analysis analysis,
      PlanVariableAllocator variableAllocator,
      PlanNodeIdAllocator idAllocator,
      Metadata metadata,
      Session session,
      AssignmentContext context) {
    this.context = context;
    requireNonNull(analysis, "analysis is null");
    requireNonNull(variableAllocator, "variableAllocator is null");
    requireNonNull(idAllocator, "idAllocator is null");
    requireNonNull(metadata, "metadata is null");
    requireNonNull(session, "session is null");

    this.analysis = analysis;
    this.variableAllocator = variableAllocator;
    this.idAllocator = idAllocator;
    this.metadata = metadata;
    this.session = session;
    this.subqueryPlanner = new SubqueryPlanner(analysis, variableAllocator, idAllocator, metadata, session,
        context);
  }

  @Override
  protected RelationPlan visitTable(Table node, AssignmentContext context) {
    Scope scope = analysis.getScope(node, context);

    TableHandle handle = analysis.getTableHandle(node);

    ImmutableList.Builder<VariableReferenceExpression> outputVariablesBuilder = ImmutableList
        .builder();
    ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> columns = ImmutableMap
        .builder();
    for (Field field : scope.getRelationType().getAllFields()) {
      VariableReferenceExpression variable = variableAllocator
          .newVariable(field.getName().get(), field.getType());
      outputVariablesBuilder.add(variable);
      columns.put(variable, analysis.getColumn(field));
    }

    List<VariableReferenceExpression> outputVariables = outputVariablesBuilder.build();
    PlanNode root = new TableScanNode(idAllocator.getNextId(), handle, outputVariables,
        columns.build(), TupleDomain.all(), TupleDomain
        .all());
    return new RelationPlan(root, scope, outputVariables);
  }

  @Override
  protected RelationPlan visitAliasedRelation(AliasedRelation node, AssignmentContext context) {
    RelationPlan subPlan = process(node.getRelation(), context);

    PlanNode root = subPlan.getRoot();
    List<VariableReferenceExpression> mappings = subPlan.getFieldMappings();

    if (node.getColumnNames() != null) {
      ImmutableList.Builder<VariableReferenceExpression> newMappings = ImmutableList.builder();
      Assignments.Builder assignments = Assignments.builder();

      // project only the visible columns from the underlying relation
      for (int i = 0; i < subPlan.getDescriptor().getAllFieldCount(); i++) {
        Field field = subPlan.getDescriptor().getFieldByIndex(i);
        if (!field.isHidden()) {
          VariableReferenceExpression aliasedColumn = variableAllocator.newVariable(field);
          assignments.put(aliasedColumn,
              castToRowExpression(asSymbolReference(subPlan.getFieldMappings().get(i))));
          newMappings.add(aliasedColumn);
        }
      }

      root = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build(),
          LOCAL);
      mappings = newMappings.build();
    }

    return new RelationPlan(root, analysis.getScope(node, context), mappings);
  }

  @Override
  protected RelationPlan visitJoin(Join node, AssignmentContext context) {
    // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
    RelationPlan leftPlan = process(node.getLeft(), context);
    RelationPlan rightPlan = process(node.getRight(), context);

    PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
    PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);

    // NOTE: variables must be in the same order as the outputDescriptor
    List<VariableReferenceExpression> outputs = ImmutableList.<VariableReferenceExpression>builder()
        .addAll(leftPlan.getFieldMappings())
        .addAll(rightPlan.getFieldMappings())
        .build();

    ImmutableList.Builder<JoinNode.EquiJoinClause> equiClauses = ImmutableList.builder();
    List<Expression> complexJoinExpressions = new ArrayList<>();
    List<Expression> postInnerJoinConditions = new ArrayList<>();

    if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
      Expression criteria = analysis.getJoinCriteria(node);

      RelationType left = analysis.getOutputDescriptor(node.getLeft());
      RelationType right = analysis.getOutputDescriptor(node.getRight());

      List<Expression> leftComparisonExpressions = new ArrayList<>();
      List<Expression> rightComparisonExpressions = new ArrayList<>();
      List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();

      for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
        conjunct = ExpressionUtils.normalize(conjunct);

        if (!isEqualComparisonExpression(conjunct) && node.getType() != INNER) {
          complexJoinExpressions.add(conjunct);
          continue;
        }

        Set<QualifiedName> dependencies = VariablesExtractor
            .extractNames(conjunct, analysis.getColumnReferences());

        if (dependencies.stream().allMatch(left::canResolve) || dependencies.stream()
            .allMatch(right::canResolve)) {
          // If the conjunct can be evaluated entirely with the inputs on either side of the join, add
          // it to the list complex expressions and let the optimizers figure out how to push it down later.
          complexJoinExpressions.add(conjunct);
        } else if (conjunct instanceof ComparisonExpression) {
          Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
          Expression secondExpression = ((ComparisonExpression) conjunct).getRight();
          ComparisonExpression.Operator comparisonOperator = ((ComparisonExpression) conjunct)
              .getOperator();
          Set<QualifiedName> firstDependencies = VariablesExtractor
              .extractNames(firstExpression, analysis.getColumnReferences());
          Set<QualifiedName> secondDependencies = VariablesExtractor
              .extractNames(secondExpression, analysis.getColumnReferences());

          if (firstDependencies.stream().allMatch(left::canResolve) && secondDependencies.stream()
              .allMatch(right::canResolve)) {
            leftComparisonExpressions.add(firstExpression);
            rightComparisonExpressions.add(secondExpression);
            addNullFilters(complexJoinExpressions, node.getType(), firstExpression,
                secondExpression);
            joinConditionComparisonOperators.add(comparisonOperator);
          } else if (firstDependencies.stream().allMatch(right::canResolve) && secondDependencies
              .stream().allMatch(left::canResolve)) {
            leftComparisonExpressions.add(secondExpression);
            rightComparisonExpressions.add(firstExpression);
            addNullFilters(complexJoinExpressions, node.getType(), secondExpression,
                firstExpression);
            joinConditionComparisonOperators.add(comparisonOperator.flip());
          } else {
            // the case when we mix variables from both left and right join side on either side of condition.
            complexJoinExpressions.add(conjunct);
          }
        } else {
          complexJoinExpressions.add(conjunct);
        }
      }

      leftPlanBuilder = subqueryPlanner
          .handleSubqueries(leftPlanBuilder, leftComparisonExpressions, node);
      rightPlanBuilder = subqueryPlanner
          .handleSubqueries(rightPlanBuilder, rightComparisonExpressions, node);

      // Add projections for join criteria
      leftPlanBuilder = leftPlanBuilder
          .appendProjections(leftComparisonExpressions, variableAllocator, idAllocator);
      rightPlanBuilder = rightPlanBuilder
          .appendProjections(rightComparisonExpressions, variableAllocator, idAllocator);

      for (int i = 0; i < leftComparisonExpressions.size(); i++) {
        if (joinConditionComparisonOperators.get(i) == ComparisonExpression.Operator.EQUAL) {
          VariableReferenceExpression leftVariable = leftPlanBuilder
              .translateToVariable(leftComparisonExpressions.get(i));
          VariableReferenceExpression righVariable = rightPlanBuilder
              .translateToVariable(rightComparisonExpressions.get(i));

          equiClauses.add(new JoinNode.EquiJoinClause(leftVariable, righVariable));
        } else {
          Expression leftExpression = leftPlanBuilder.rewrite(leftComparisonExpressions.get(i));
          Expression rightExpression = rightPlanBuilder.rewrite(rightComparisonExpressions.get(i));
          postInnerJoinConditions.add(
              new ComparisonExpression(joinConditionComparisonOperators.get(i), leftExpression,
                  rightExpression));
        }
      }
    }

    PlanNode root = new JoinNode(idAllocator.getNextId(),
        JoinNodeUtils.typeConvert(node.getType()),
        leftPlanBuilder.getRoot(),
        rightPlanBuilder.getRoot(),
        equiClauses.build(),
        ImmutableList.<VariableReferenceExpression>builder()
            .addAll(leftPlanBuilder.getRoot().getOutputVariables())
            .addAll(rightPlanBuilder.getRoot().getOutputVariables())
            .build(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        Optional.empty(),
        ImmutableMap.of());

    if (node.getType() != INNER) {
      for (Expression complexExpression : complexJoinExpressions) {
        Set<InPredicate> inPredicates = subqueryPlanner
            .collectInPredicateSubqueries(complexExpression, node);
        if (!inPredicates.isEmpty()) {
          InPredicate inPredicate = Iterables.getLast(inPredicates);
          throw notSupportedException(inPredicate, "IN with subquery predicate in join condition");
        }
      }

      // subqueries can be applied only to one side of join - left side is selected in arbitrary way
      leftPlanBuilder = subqueryPlanner
          .handleUncorrelatedSubqueries(leftPlanBuilder, complexJoinExpressions, node);
    }

    RelationPlan intermediateRootRelationPlan = new RelationPlan(root, analysis.getScope(node,
        context),
        outputs);
    TranslationMap translationMap = new TranslationMap(intermediateRootRelationPlan, analysis);
    translationMap.setFieldMappings(outputs);
    translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
    translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());

    if (node.getType() != INNER && !complexJoinExpressions.isEmpty()) {
      Expression joinedFilterCondition = ExpressionUtils.and(complexJoinExpressions);
      Expression rewrittenFilterCondition = translationMap.rewrite(joinedFilterCondition);
      root = new JoinNode(idAllocator.getNextId(),
          JoinNodeUtils.typeConvert(node.getType()),
          leftPlanBuilder.getRoot(),
          rightPlanBuilder.getRoot(),
          equiClauses.build(),
          ImmutableList.<VariableReferenceExpression>builder()
              .addAll(leftPlanBuilder.getRoot().getOutputVariables())
              .addAll(rightPlanBuilder.getRoot().getOutputVariables())
              .build(),
          Optional.of(castToRowExpression(rewrittenFilterCondition)),
          Optional.empty(),
          Optional.empty(),
          Optional.empty(),
          ImmutableMap.of());
    }

    if (node.getType() == INNER) {
      // rewrite all the other conditions using output variables from left + right plan node.
      PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root);
      rootPlanBuilder = subqueryPlanner
          .handleSubqueries(rootPlanBuilder, complexJoinExpressions, node);

      for (Expression expression : complexJoinExpressions) {
        postInnerJoinConditions.add(rootPlanBuilder.rewrite(expression));
      }
      root = rootPlanBuilder.getRoot();

      Expression postInnerJoinCriteria;
      if (!postInnerJoinConditions.isEmpty()) {
        postInnerJoinCriteria = ExpressionUtils.and(postInnerJoinConditions);
        root = new FilterNode(idAllocator.getNextId(), root,
            castToRowExpression(postInnerJoinCriteria));
      }
    }

    return new RelationPlan(root, analysis.getScope(node, context), outputs);
  }

  private void addNullFilters(List<Expression> conditions, Join.Type joinType, Expression left,
      Expression right) {
    if (true/*SystemSessionProperties.isOptimizeNullsInJoin(session)*/) {
      switch (joinType) {
        case INNER:
          addNullFilterIfSupported(conditions, left);
          addNullFilterIfSupported(conditions, right);
          break;
        case LEFT:
          addNullFilterIfSupported(conditions, right);
          break;
        case RIGHT:
          addNullFilterIfSupported(conditions, left);
          break;
      }
    }
  }

  private void addNullFilterIfSupported(List<Expression> conditions, Expression incoming) {
    if (!(incoming instanceof InPredicate)) {
      // (A.x IN (1,2,3)) IS NOT NULL is not supported as a join condition as of today.
      conditions.add(new IsNotNullPredicate(incoming));
    }
  }

  @Override
  protected RelationPlan visitTableSubquery(TableSubquery node, AssignmentContext context) {
    return process(node.getQuery(), context);
  }

  @Override
  protected RelationPlan visitQuery(Query node, AssignmentContext context) {
    return new QueryPlanner(analysis, variableAllocator, idAllocator, metadata, session, context)
        .plan(node);
  }

  @Override
  protected RelationPlan visitQuerySpecification(QuerySpecification node, AssignmentContext context) {
    return new QueryPlanner(analysis, variableAllocator, idAllocator, metadata, session, context)
        .plan(node);
  }

  private RelationPlan processAndCoerceIfNecessary(Relation node, AssignmentContext context) {
    Type[] coerceToTypes = analysis.getRelationCoercion(node);

    RelationPlan plan = this.process(node, context);

    if (coerceToTypes == null) {
      return plan;
    }

    return addCoercions(plan, coerceToTypes);
  }

  private RelationPlan addCoercions(RelationPlan plan, Type[] targetColumnTypes) {
    RelationType oldRelation = plan.getDescriptor();
    List<VariableReferenceExpression> oldVisibleVariables = oldRelation.getVisibleFields().stream()
        .map(oldRelation::indexOf)
        .map(plan.getFieldMappings()::get)
        .collect(toImmutableList());
    RelationType oldRelationWithVisibleFields = plan.getDescriptor().withOnlyVisibleFields();
    verify(targetColumnTypes.length == oldVisibleVariables.size());
    ImmutableList.Builder<VariableReferenceExpression> newVariables = new ImmutableList.Builder<>();
    Field[] newFields = new Field[targetColumnTypes.length];
    Assignments.Builder assignments = Assignments.builder();
    for (int i = 0; i < targetColumnTypes.length; i++) {
      VariableReferenceExpression inputVariable = oldVisibleVariables.get(i);
      Type outputType = targetColumnTypes[i];
      if (!outputType.equals(inputVariable.getType())) {
        Expression cast = new Cast(new SymbolReference(inputVariable.getName()),
            outputType.getTypeSignature().toString());
        VariableReferenceExpression outputVariable = variableAllocator
            .newVariable(cast, outputType);
        assignments.put(outputVariable, castToRowExpression(cast));
        newVariables.add(outputVariable);
      } else {
        SymbolReference symbolReference = new SymbolReference(inputVariable.getName());
        VariableReferenceExpression outputVariable = variableAllocator
            .newVariable(symbolReference, outputType);
        assignments.put(outputVariable, castToRowExpression(symbolReference));
        newVariables.add(outputVariable);
      }
      Field oldField = oldRelationWithVisibleFields.getFieldByIndex(i);
      newFields[i] = new Field(
          oldField.getRelationAlias(),
          oldField.getName(),
          targetColumnTypes[i],
          oldField.isHidden(),
          oldField.getOriginTable(),
          oldField.getOriginColumnName(),
          oldField.isAliased());
    }
    ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(),
        assignments.build());
    return new RelationPlan(projectNode,
        Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(newFields))
            .build(), newVariables.build());
  }

  @Override
  protected RelationPlan visitUnion(Union node, AssignmentContext context) {
    checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");

    SetOperationPlan setOperationPlan = process(node);

    PlanNode planNode = new UnionNode(idAllocator.getNextId(), setOperationPlan.getSources(),
        setOperationPlan.getOutputVariables(), setOperationPlan.getVariableMapping());
    if (node.isDistinct().orElse(true)) {
      planNode = distinct(planNode);
    }
    return new RelationPlan(planNode, analysis.getScope(node, context), planNode.getOutputVariables());
  }

  @Override
  protected RelationPlan visitIntersect(Intersect node, AssignmentContext context) {
    checkArgument(!node.getRelations().isEmpty(), "No relations specified for INTERSECT");

    SetOperationPlan setOperationPlan = process(node);

    PlanNode planNode = new IntersectNode(idAllocator.getNextId(), setOperationPlan.getSources(),
        setOperationPlan.getOutputVariables(), setOperationPlan.getVariableMapping());
    return new RelationPlan(planNode, analysis.getScope(node, context), planNode.getOutputVariables());
  }

  @Override
  protected RelationPlan visitExcept(Except node, AssignmentContext context) {
    checkArgument(!node.getRelations().isEmpty(), "No relations specified for EXCEPT");

    SetOperationPlan setOperationPlan = process(node);

    PlanNode planNode = new ExceptNode(idAllocator.getNextId(), setOperationPlan.getSources(),
        setOperationPlan.getOutputVariables(), setOperationPlan.getVariableMapping());
    return new RelationPlan(planNode, analysis.getScope(node, context), planNode.getOutputVariables());
  }

  private SetOperationPlan process(SetOperation node) {
    List<VariableReferenceExpression> outputs = null;
    ImmutableList.Builder<PlanNode> sources = ImmutableList.builder();
    ImmutableListMultimap.Builder<VariableReferenceExpression, VariableReferenceExpression> variableMapping = ImmutableListMultimap
        .builder();

    List<RelationPlan> subPlans = node.getRelations().stream()
        .map(relation -> processAndCoerceIfNecessary(relation, null))
        .collect(toImmutableList());

    for (RelationPlan relationPlan : subPlans) {
      List<VariableReferenceExpression> childOutputVariables = relationPlan.getFieldMappings();
      if (outputs == null) {
        // Use the first Relation to derive output variable names
        RelationType descriptor = relationPlan.getDescriptor();
        ImmutableList.Builder<VariableReferenceExpression> outputVariableBuilder = ImmutableList
            .builder();
        for (Field field : descriptor.getVisibleFields()) {
          int fieldIndex = descriptor.indexOf(field);
          VariableReferenceExpression variable = childOutputVariables.get(fieldIndex);
          outputVariableBuilder.add(variableAllocator.newVariable(variable));
        }
        outputs = outputVariableBuilder.build();
      }

      RelationType descriptor = relationPlan.getDescriptor();
      checkArgument(descriptor.getVisibleFieldCount() == outputs.size(),
          "Expected relation to have %s variables but has %s variables",
          descriptor.getVisibleFieldCount(),
          outputs.size());

      int fieldId = 0;
      for (Field field : descriptor.getVisibleFields()) {
        int fieldIndex = descriptor.indexOf(field);
        variableMapping.put(outputs.get(fieldId), childOutputVariables.get(fieldIndex));
        fieldId++;
      }

      sources.add(relationPlan.getRoot());
    }

    return new SetOperationPlan(sources.build(), variableMapping.build());
  }

  private PlanBuilder initializePlanBuilder(RelationPlan relationPlan) {
    TranslationMap translations = new TranslationMap(relationPlan, analysis);

    // Make field->variable mapping from underlying relation plan available for translations
    // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
    translations.setFieldMappings(relationPlan.getFieldMappings());

    return new PlanBuilder(translations, relationPlan.getRoot());
  }

  private PlanNode distinct(PlanNode node) {
    return new AggregationNode(idAllocator.getNextId(),
        node,
        ImmutableMap.of(),
        singleGroupingSet(node.getOutputVariables()),
        ImmutableList.of(),
        AggregationNode.Step.SINGLE,
        Optional.empty(),
        Optional.empty());
  }

  private static class SetOperationPlan {

    private final List<PlanNode> sources;
    private final List<VariableReferenceExpression> outputVariables;
    private final Map<VariableReferenceExpression, List<VariableReferenceExpression>> variableMapping;

    private SetOperationPlan(List<PlanNode> sources,
        ListMultimap<VariableReferenceExpression, VariableReferenceExpression> variableMapping) {
      this.sources = sources;
      this.outputVariables = ImmutableList.copyOf(variableMapping.keySet());
      Map<VariableReferenceExpression, List<VariableReferenceExpression>> mapping = new LinkedHashMap<>();
      variableMapping.asMap().forEach((key, value) -> {
        checkState(value instanceof List, "variableMapping values should be of type List");
        mapping.put(key, (List<VariableReferenceExpression>) value);
      });
      this.variableMapping = mapping;
    }

    public List<PlanNode> getSources() {
      return sources;
    }

    public List<VariableReferenceExpression> getOutputVariables() {
      return outputVariables;
    }

    public Map<VariableReferenceExpression, List<VariableReferenceExpression>> getVariableMapping() {
      return variableMapping;
    }
  }
}
