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

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.analyzer.ColumnHandle;
import ai.dataeng.sqml.analyzer.Scope;
import ai.dataeng.sqml.analyzer.StatementAnalysis;
import ai.dataeng.sqml.analyzer.TableHandle;
import ai.dataeng.sqml.logical4.DocumentComputeOperator;
import ai.dataeng.sqml.logical4.DocumentComputeOperator.Computation;
import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.LogicalPlan.Column;
import ai.dataeng.sqml.logical4.LogicalPlan.Node;
import ai.dataeng.sqml.logical4.ShreddingOperator;
import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.physical.flink.FieldProjection;
import ai.dataeng.sqml.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.tree.DefaultTraversalVisitor;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.util.List;

class RelationPlanner
        extends DefaultTraversalVisitor<RelationPlan, Void>
{
    private final StatementAnalysis analysis;
    private final PlanVariableAllocator variableAllocator;
    private final RowNodeIdAllocator idAllocator;
    private final Metadata metadata;
//    private final Session session;
//    private final SubqueryPlanner subqueryPlanner;

    RelationPlanner(
            StatementAnalysis analysis,
            PlanVariableAllocator variableAllocator,
            RowNodeIdAllocator idAllocator,
            Metadata metadata
    )
    {
        requireNonNull(analysis, "analysis is null");
        requireNonNull(variableAllocator, "variableAllocator is null");
        requireNonNull(idAllocator, "idAllocator is null");
        requireNonNull(metadata, "metadata is null");

        this.analysis = analysis;
        this.variableAllocator = variableAllocator;
        this.idAllocator = idAllocator;
        this.metadata = metadata;
    }

    @Override
    public RelationPlan visitTable(Table node, Void context)
    {
//        Query namedQuery = analysis.getNamedQuery(node);
        Scope scope = analysis.getScope(node);
//
//        if (namedQuery != null) {
//            RelationPlan subPlan = process(namedQuery, null);
//
//            // Add implicit coercions if view query produces types that don't match the declared output types
//            // of the view (e.g., if the underlying tables referenced by the view changed)
//            Type[] types = scope.getRelationType().getAllFields().stream().map(Field::getType).toArray(Type[]::new);
////            RelationPlan withCoercions = addCoercions(subPlan, types);
//            return new RelationPlan(subPlan.getRoot(), scope, subPlan.getFieldMappings());
//        }

        TableHandle handle = analysis.getTableHandle(node);

        ImmutableList.Builder<ColumnReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
        ImmutableMap.Builder<ColumnReferenceExpression, ColumnHandle> columns = ImmutableMap.builder();
        for (Field field : scope.getRelation().getScalarFields()) {
            ColumnReferenceExpression variable = variableAllocator.newVariable(field.getName().getCanonical(), field.getType());
            outputVariablesBuilder.add(variable);
            ColumnHandle columnHandle = analysis.getColumn(field);
            if (columnHandle == null) {
                System.out.println(field);
                continue;
            }
            columns.put(variable, columnHandle);
        }

        List<ColumnReferenceExpression> outputVariables = outputVariablesBuilder.build();
        //Todo: lookup operator in operator catalog
        Node root = new ShreddingOperator(null, null, new FieldProjection[0], new Column[0]);//idAllocator.getNextId(), handle, outputVariables, columns.build());
        return new RelationPlan(root, scope, outputVariables);
    }
//
//    @Override
//    public RelationPlan visitAliasedRelation(AliasedRelation node, Void context)
//    {
//        RelationPlan subPlan = process(node.getRelation(), context);
//
//        RowNode root = subPlan.getRoot();
//        List<ColumnReferenceExpression> mappings = subPlan.getFieldMappings();
//
//        if (node.getColumnNames() != null) {
//            ImmutableList.Builder<ColumnReferenceExpression> newMappings = ImmutableList.builder();
//            Assignments.Builder assignments = Assignments.builder();
//
//            // project only the visible columns from the underlying relation
//            for (int i = 0; i < subPlan.getDescriptor().getAllFieldCount(); i++) {
//                Field field = subPlan.getDescriptor().getFieldByIndex(i);
//                if (!field.isHidden()) {
//                    ColumnReferenceExpression aliasedColumn = variableAllocator.newVariable(field);
//                    assignments.put(aliasedColumn, castToRowExpression(asSymbolReference(subPlan.getFieldMappings().get(i))));
//                    newMappings.add(aliasedColumn);
//                }
//            }
//
//            root = new ProjectNode(idAllocator.getNextId(), subPlan.getRoot(), assignments.build(), LOCAL);
//            mappings = newMappings.build();
//        }
//
//        return new RelationPlan(root, analysis.getScope(node), mappings);
//    }
//
//    @Override
//    public RelationPlan visitSampledRelation(SampledRelation node, Void context)
//    {
//        RelationPlan subPlan = process(node.getRelation(), context);
//
//        double ratio = analysis.getSampleRatio(node);
//        RowNode RowNode = new SampleNode(idAllocator.getNextId(),
//                subPlan.getRoot(),
//                ratio,
//                SampleNodeUtil.fromType(node.getType()));
//        return new RelationPlan(RowNode, analysis.getScope(node), subPlan.getFieldMappings());
//    }
//
//    @Override
//    public RelationPlan visitJoin(Join node, Void context)
//    {
//        // TODO: translate the RIGHT join into a mirrored LEFT join when we refactor (@martint)
//        RelationPlan leftPlan = process(node.getLeft(), context);
//
//        Optional<Unnest> unnest = getUnnest(node.getRight());
//        if (unnest.isPresent()) {
//            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
//                throw notSupportedException(unnest.get(), "UNNEST on other than the right side of CROSS JOIN");
//            }
//            return planCrossJoinUnnest(leftPlan, node, unnest.get());
//        }
//
//        Optional<Lateral> lateral = getLateral(node.getRight());
//        if (lateral.isPresent()) {
//            if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
//                throw notSupportedException(lateral.get(), "LATERAL on other than the right side of CROSS JOIN");
//            }
//            return planLateralJoin(node, leftPlan, lateral.get());
//        }
//
//        RelationPlan rightPlan = process(node.getRight(), context);
//
//        if (node.getCriteria().isPresent() && node.getCriteria().get() instanceof JoinUsing) {
//            return planJoinUsing(node, leftPlan, rightPlan);
//        }
//
//        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
//        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);
//
//        // NOTE: variables must be in the same order as the outputDescriptor
//        List<ColumnReferenceExpression> outputs = ImmutableList.<ColumnReferenceExpression>builder()
//                .addAll(leftPlan.getFieldMappings())
//                .addAll(rightPlan.getFieldMappings())
//                .build();
//
//        ImmutableList.Builder<JoinNode.EquiJoinClause> equiClauses = ImmutableList.builder();
//        List<Expression> complexJoinExpressions = new ArrayList<>();
//        List<Expression> postInnerJoinConditions = new ArrayList<>();
//
//        if (node.getType() != Join.Type.CROSS && node.getType() != Join.Type.IMPLICIT) {
//            Expression criteria = analysis.getJoinCriteria(node);
//
//            RelationType left = analysis.getOutputDescriptor(node.getLeft());
//            RelationType right = analysis.getOutputDescriptor(node.getRight());
//
//            List<Expression> leftComparisonExpressions = new ArrayList<>();
//            List<Expression> rightComparisonExpressions = new ArrayList<>();
//            List<ComparisonExpression.Operator> joinConditionComparisonOperators = new ArrayList<>();
//
//            for (Expression conjunct : ExpressionUtils.extractConjuncts(criteria)) {
//                conjunct = ExpressionUtils.normalize(conjunct);
//
//                if (!isEqualComparisonExpression(conjunct) && node.getType() != INNER) {
//                    complexJoinExpressions.add(conjunct);
//                    continue;
//                }
//
//                Set<QualifiedName> dependencies = VariablesExtractor.extractNames(conjunct, analysis.getColumnReferences());
//
//                if (dependencies.stream().allMatch(left::canResolve) || dependencies.stream().allMatch(right::canResolve)) {
//                    // If the conjunct can be evaluated entirely with the inputs on either side of the join, add
//                    // it to the list complex expressions and let the optimizers figure out how to push it down later.
//                    complexJoinExpressions.add(conjunct);
//                }
//                else if (conjunct instanceof ComparisonExpression) {
//                    Expression firstExpression = ((ComparisonExpression) conjunct).getLeft();
//                    Expression secondExpression = ((ComparisonExpression) conjunct).getRight();
//                    ComparisonExpression.Operator comparisonOperator = ((ComparisonExpression) conjunct).getOperator();
//                    Set<QualifiedName> firstDependencies = VariablesExtractor.extractNames(firstExpression, analysis.getColumnReferences());
//                    Set<QualifiedName> secondDependencies = VariablesExtractor.extractNames(secondExpression, analysis.getColumnReferences());
//
//                    if (firstDependencies.stream().allMatch(left::canResolve) && secondDependencies.stream().allMatch(right::canResolve)) {
//                        leftComparisonExpressions.add(firstExpression);
//                        rightComparisonExpressions.add(secondExpression);
//                        addNullFilters(complexJoinExpressions, node.getType(), firstExpression, secondExpression);
//                        joinConditionComparisonOperators.add(comparisonOperator);
//                    }
//                    else if (firstDependencies.stream().allMatch(right::canResolve) && secondDependencies.stream().allMatch(left::canResolve)) {
//                        leftComparisonExpressions.add(secondExpression);
//                        rightComparisonExpressions.add(firstExpression);
//                        addNullFilters(complexJoinExpressions, node.getType(), secondExpression, firstExpression);
//                        joinConditionComparisonOperators.add(comparisonOperator.flip());
//                    }
//                    else {
//                        // the case when we mix variables from both left and right join side on either side of condition.
//                        complexJoinExpressions.add(conjunct);
//                    }
//                }
//                else {
//                    complexJoinExpressions.add(conjunct);
//                }
//            }
//
//            leftPlanBuilder = subqueryPlanner.handleSubqueries(leftPlanBuilder, leftComparisonExpressions, node);
//            rightPlanBuilder = subqueryPlanner.handleSubqueries(rightPlanBuilder, rightComparisonExpressions, node);
//
//            // Add projections for join criteria
//            leftPlanBuilder = leftPlanBuilder.appendProjections(leftComparisonExpressions, variableAllocator, idAllocator);
//            rightPlanBuilder = rightPlanBuilder.appendProjections(rightComparisonExpressions, variableAllocator, idAllocator);
//
//            for (int i = 0; i < leftComparisonExpressions.size(); i++) {
//                if (joinConditionComparisonOperators.get(i) == ComparisonExpression.Operator.EQUAL) {
//                    ColumnReferenceExpression leftVariable = leftPlanBuilder.translateToVariable(leftComparisonExpressions.get(i));
//                    ColumnReferenceExpression righVariable = rightPlanBuilder.translateToVariable(rightComparisonExpressions.get(i));
//
//                    equiClauses.add(new JoinNode.EquiJoinClause(leftVariable, righVariable));
//                }
//                else {
//                    Expression leftExpression = leftPlanBuilder.rewrite(leftComparisonExpressions.get(i));
//                    Expression rightExpression = rightPlanBuilder.rewrite(rightComparisonExpressions.get(i));
//                    postInnerJoinConditions.add(new ComparisonExpression(joinConditionComparisonOperators.get(i), leftExpression, rightExpression));
//                }
//            }
//        }
//
//        RowNode root = new JoinNode(idAllocator.getNextId(),
//                JoinNodeUtils.typeConvert(node.getType()),
//                leftPlanBuilder.getRoot(),
//                rightPlanBuilder.getRoot(),
//                equiClauses.build(),
//                ImmutableList.<ColumnReferenceExpression>builder()
//                        .addAll(leftPlanBuilder.getRoot().getOutputVariables())
//                        .addAll(rightPlanBuilder.getRoot().getOutputVariables())
//                        .build(),
//                Optional.empty(),
//                Optional.empty(),
//                Optional.empty(),
//                Optional.empty(),
//                ImmutableMap.of());
//
//        if (node.getType() != INNER) {
//            for (Expression complexExpression : complexJoinExpressions) {
//                Set<InPredicate> inPredicates = subqueryPlanner.collectInPredicateSubqueries(complexExpression, node);
//                if (!inPredicates.isEmpty()) {
//                    InPredicate inPredicate = Iterables.getLast(inPredicates);
//                    throw notSupportedException(inPredicate, "IN with subquery predicate in join condition");
//                }
//            }
//
//            // subqueries can be applied only to one side of join - left side is selected in arbitrary way
//            leftPlanBuilder = subqueryPlanner.handleUncorrelatedSubqueries(leftPlanBuilder, complexJoinExpressions, node);
//        }
//
//        RelationPlan intermediateRootRelationPlan = new RelationPlan(root, analysis.getScope(node), outputs);
//        TranslationMap translationMap = new TranslationMap(intermediateRootRelationPlan, analysis, lambdaDeclarationToVariableMap);
//        translationMap.setFieldMappings(outputs);
//        translationMap.putExpressionMappingsFrom(leftPlanBuilder.getTranslations());
//        translationMap.putExpressionMappingsFrom(rightPlanBuilder.getTranslations());
//
//        if (node.getType() != INNER && !complexJoinExpressions.isEmpty()) {
//            Expression joinedFilterCondition = ExpressionUtils.and(complexJoinExpressions);
//            Expression rewrittenFilterCondition = translationMap.rewrite(joinedFilterCondition);
//            root = new JoinNode(idAllocator.getNextId(),
//                    JoinNodeUtils.typeConvert(node.getType()),
//                    leftPlanBuilder.getRoot(),
//                    rightPlanBuilder.getRoot(),
//                    equiClauses.build(),
//                    ImmutableList.<ColumnReferenceExpression>builder()
//                            .addAll(leftPlanBuilder.getRoot().getOutputVariables())
//                            .addAll(rightPlanBuilder.getRoot().getOutputVariables())
//                            .build(),
//                    Optional.of(castToRowExpression(rewrittenFilterCondition)),
//                    Optional.empty(),
//                    Optional.empty(),
//                    Optional.empty(),
//                    ImmutableMap.of());
//        }
//
//        if (node.getType() == INNER) {
//            // rewrite all the other conditions using output variables from left + right plan node.
//            PlanBuilder rootPlanBuilder = new PlanBuilder(translationMap, root);
//            rootPlanBuilder = subqueryPlanner.handleSubqueries(rootPlanBuilder, complexJoinExpressions, node);
//
//            for (Expression expression : complexJoinExpressions) {
//                postInnerJoinConditions.add(rootPlanBuilder.rewrite(expression));
//            }
//            root = rootPlanBuilder.getRoot();
//
//            Expression postInnerJoinCriteria;
//            if (!postInnerJoinConditions.isEmpty()) {
//                postInnerJoinCriteria = ExpressionUtils.and(postInnerJoinConditions);
//                root = new FilterNode(idAllocator.getNextId(), root, castToRowExpression(postInnerJoinCriteria));
//            }
//        }
//
//        return new RelationPlan(root, analysis.getScope(node), outputs);
//    }
//
//    private void addNullFilters(List<Expression> conditions, Join.Type joinType, Expression left, Expression right)
//    {
//        if (SystemSessionProperties.isOptimizeNullsInJoin(session)) {
//            switch (joinType) {
//                case INNER:
//                    addNullFilterIfSupported(conditions, left);
//                    addNullFilterIfSupported(conditions, right);
//                    break;
//                case LEFT:
//                    addNullFilterIfSupported(conditions, right);
//                    break;
//                case RIGHT:
//                    addNullFilterIfSupported(conditions, left);
//                    break;
//            }
//        }
//    }
//
//    private void addNullFilterIfSupported(List<Expression> conditions, Expression incoming)
//    {
//        if (!(incoming instanceof InPredicate)) {
//            // (A.x IN (1,2,3)) IS NOT NULL is not supported as a join condition as of today.
//            conditions.add(new IsNotNullPredicate(incoming));
//        }
//    }
//
//    private RelationPlan planJoinUsing(Join node, RelationPlan left, RelationPlan right)
//    {
//        /* Given: l JOIN r USING (k1, ..., kn)
//
//           produces:
//
//            - project
//                    coalesce(l.k1, r.k1)
//                    ...,
//                    coalesce(l.kn, r.kn)
//                    l.v1,
//                    ...,
//                    l.vn,
//                    r.v1,
//                    ...,
//                    r.vn
//              - join (l.k1 = r.k1 and ... l.kn = r.kn)
//                    - project
//                        cast(l.k1 as commonType(l.k1, r.k1))
//                        ...
//                    - project
//                        cast(rl.k1 as commonType(l.k1, r.k1))
//
//            If casts are redundant (due to column type and common type being equal),
//            they will be removed by optimization passes.
//        */
//
//        List<Identifier> joinColumns = ((JoinUsing) node.getCriteria().get()).getColumns();
//
//        StatementAnalysis.JoinUsingStatementAnalysis joinStatementAnalysis = analysis.getJoinUsing(node);
//
//        ImmutableList.Builder<JoinNode.EquiJoinClause> clauses = ImmutableList.builder();
//
//        Map<Identifier, ColumnReferenceExpression> leftJoinColumns = new HashMap<>();
//        Map<Identifier, ColumnReferenceExpression> rightJoinColumns = new HashMap<>();
//
//        Assignments.Builder leftCoercions = Assignments.builder();
//        Assignments.Builder rightCoercions = Assignments.builder();
//
//        leftCoercions.putAll(identitiesAsSymbolReferences(left.getRoot().getOutputVariables()));
//        rightCoercions.putAll(identitiesAsSymbolReferences(right.getRoot().getOutputVariables()));
//        for (int i = 0; i < joinColumns.size(); i++) {
//            Identifier identifier = joinColumns.get(i);
//            Type type = analysis.getType(identifier);
//
//            // compute the coercion for the field on the left to the common supertype of left & right
//            ColumnReferenceExpression leftOutput = variableAllocator.newVariable(identifier, type);
//            int leftField = joinStatementAnalysis.getLeftJoinFields().get(i);
//            leftCoercions.put(leftOutput, castToRowExpression(new Cast(
//                    new SymbolReference(left.getVariable(leftField).getName()),
//                    type.getTypeSignature().toString(),
//                    false,
//                    metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(left.getDescriptor().getFieldByIndex(leftField).getType(), type))));
//            leftJoinColumns.put(identifier, leftOutput);
//
//            // compute the coercion for the field on the right to the common supertype of left & right
//            ColumnReferenceExpression rightOutput = variableAllocator.newVariable(identifier, type);
//            int rightField = joinStatementAnalysis.getRightJoinFields().get(i);
//            rightCoercions.put(rightOutput, castToRowExpression(new Cast(
//                    new SymbolReference(right.getVariable(rightField).getName()),
//                    type.getTypeSignature().toString(),
//                    false,
//                    metadata.getFunctionAndTypeManager().isTypeOnlyCoercion(right.getDescriptor().getFieldByIndex(rightField).getType(), type))));
//            rightJoinColumns.put(identifier, rightOutput);
//
//            clauses.add(new JoinNode.EquiJoinClause(leftOutput, rightOutput));
//        }
//
//        ProjectNode leftCoercion = new ProjectNode(idAllocator.getNextId(), left.getRoot(), leftCoercions.build());
//        ProjectNode rightCoercion = new ProjectNode(idAllocator.getNextId(), right.getRoot(), rightCoercions.build());
//
//        JoinNode join = new JoinNode(
//                idAllocator.getNextId(),
//                JoinNodeUtils.typeConvert(node.getType()),
//                leftCoercion,
//                rightCoercion,
//                clauses.build(),
//                ImmutableList.<ColumnReferenceExpression>builder()
//                        .addAll(leftCoercion.getOutputVariables())
//                        .addAll(rightCoercion.getOutputVariables())
//                        .build(),
//                Optional.empty(),
//                Optional.empty(),
//                Optional.empty(),
//                Optional.empty(),
//                ImmutableMap.of());
//
//        // Add a projection to produce the outputs of the columns in the USING clause,
//        // which are defined as coalesce(l.k, r.k)
//        Assignments.Builder assignments = Assignments.builder();
//
//        ImmutableList.Builder<ColumnReferenceExpression> outputs = ImmutableList.builder();
//        for (Identifier column : joinColumns) {
//            ColumnReferenceExpression output = variableAllocator.newVariable(column, analysis.getType(column));
//            outputs.add(output);
//            assignments.put(output, castToRowExpression(new CoalesceExpression(
//                    new SymbolReference(leftJoinColumns.get(column).getName()),
//                    new SymbolReference(rightJoinColumns.get(column).getName()))));
//        }
//
//        for (int field : joinStatementAnalysis.getOtherLeftFields()) {
//            ColumnReferenceExpression variable = left.getFieldMappings().get(field);
//            outputs.add(variable);
//            assignments.put(variable, castToRowExpression(new SymbolReference(variable.getName())));
//        }
//
//        for (int field : joinStatementAnalysis.getOtherRightFields()) {
//            ColumnReferenceExpression variable = right.getFieldMappings().get(field);
//            outputs.add(variable);
//            assignments.put(variable, castToRowExpression(new SymbolReference(variable.getName())));
//        }
//
//        return new RelationPlan(
//                new ProjectNode(idAllocator.getNextId(), join, assignments.build()),
//                analysis.getScope(node),
//                outputs.build());
//    }
//
//    private Optional<Unnest> getUnnest(Relation relation)
//    {
//        if (relation instanceof AliasedRelation) {
//            return getUnnest(((AliasedRelation) relation).getRelation());
//        }
//        if (relation instanceof Unnest) {
//            return Optional.of((Unnest) relation);
//        }
//        return Optional.empty();
//    }
//
//    private Optional<Lateral> getLateral(Relation relation)
//    {
//        if (relation instanceof AliasedRelation) {
//            return getLateral(((AliasedRelation) relation).getRelation());
//        }
//        if (relation instanceof Lateral) {
//            return Optional.of((Lateral) relation);
//        }
//        return Optional.empty();
//    }
//
//    private RelationPlan planLateralJoin(Join join, RelationPlan leftPlan, Lateral lateral)
//    {
//        RelationPlan rightPlan = process(lateral.getQuery(), null);
//        PlanBuilder leftPlanBuilder = initializePlanBuilder(leftPlan);
//        PlanBuilder rightPlanBuilder = initializePlanBuilder(rightPlan);
//
//        PlanBuilder planBuilder = subqueryPlanner.appendLateralJoin(leftPlanBuilder, rightPlanBuilder, lateral.getQuery(), true, LateralJoinNode.Type.INNER);
//
//        List<ColumnReferenceExpression> outputVariables = ImmutableList.<ColumnReferenceExpression>builder()
//                .addAll(leftPlan.getRoot().getOutputVariables())
//                .addAll(rightPlan.getRoot().getOutputVariables())
//                .build();
//        return new RelationPlan(planBuilder.getRoot(), analysis.getScope(join), outputVariables);
//    }
//
//    private RelationPlan planCrossJoinUnnest(RelationPlan leftPlan, Join joinNode, Unnest node)
//    {
//        RelationType unnestOutputDescriptor = analysis.getOutputDescriptor(node);
//        // Create variables for the result of unnesting
//        ImmutableList.Builder<ColumnReferenceExpression> unnestedVariablesBuilder = ImmutableList.builder();
//        for (Field field : unnestOutputDescriptor.getVisibleFields()) {
//            ColumnReferenceExpression variable = variableAllocator.newVariable(field);
//            unnestedVariablesBuilder.add(variable);
//        }
//        ImmutableList<ColumnReferenceExpression> unnestedVariables = unnestedVariablesBuilder.build();
//
//        // Add a projection for all the unnest arguments
//        PlanBuilder planBuilder = initializePlanBuilder(leftPlan);
//        planBuilder = planBuilder.appendProjections(node.getExpressions(), variableAllocator, idAllocator);
//        TranslationMap translations = planBuilder.getTranslations();
//        ProjectNode projectNode = (ProjectNode) planBuilder.getRoot();
//
//        ImmutableMap.Builder<ColumnReferenceExpression, List<ColumnReferenceExpression>> unnestVariables = ImmutableMap.builder();
//        UnmodifiableIterator<ColumnReferenceExpression> unnestedVariablesIterator = unnestedVariables.iterator();
//        for (Expression expression : node.getExpressions()) {
//            Type type = analysis.getType(expression);
//            ColumnReferenceExpression inputVariable = new ColumnReferenceExpression(translations.get(expression).getName(), type);
//            if (type instanceof ArrayType) {
//                Type elementType = ((ArrayType) type).getElementType();
//                if (!SystemSessionProperties.isLegacyUnnest(session) && elementType instanceof RowType) {
//                    ImmutableList.Builder<ColumnReferenceExpression> unnestVariableBuilder = ImmutableList.builder();
//                    for (int i = 0; i < ((RowType) elementType).getFields().size(); i++) {
//                        unnestVariableBuilder.add(unnestedVariablesIterator.next());
//                    }
//                    unnestVariables.put(inputVariable, unnestVariableBuilder.build());
//                }
//                else {
//                    unnestVariables.put(inputVariable, ImmutableList.of(unnestedVariablesIterator.next()));
//                }
//            }
//            else if (type instanceof MapType) {
//                unnestVariables.put(inputVariable, ImmutableList.of(unnestedVariablesIterator.next(), unnestedVariablesIterator.next()));
//            }
//            else {
//                throw new IllegalArgumentException("Unsupported type for UNNEST: " + type);
//            }
//        }
//        Optional<ColumnReferenceExpression> ordinalityVariable = node.isWithOrdinality() ? Optional.of(unnestedVariablesIterator.next()) : Optional.empty();
//        checkState(!unnestedVariablesIterator.hasNext(), "Not all output variables were matched with input variables");
//
//        UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), projectNode, leftPlan.getFieldMappings(), unnestVariables.build(), ordinalityVariable);
//        return new RelationPlan(unnestNode, analysis.getScope(joinNode), unnestNode.getOutputVariables());
//    }
//
//    @Override
//    public RelationPlan visitTableSubquery(TableSubquery node, Void context)
//    {
//        return process(node.getQuery(), context);
//    }

    @Override
    public RelationPlan visitQuery(Query node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator, metadata)
                .plan(node);
    }

    @Override
    public RelationPlan visitQuerySpecification(QuerySpecification node, Void context)
    {
        return new QueryPlanner(analysis, variableAllocator, idAllocator, metadata)
                .plan(node);
    }
//
//    @Override
//    public RelationPlan visitValues(Values node, Void context)
//    {
//        Scope scope = analysis.getScope(node);
//        ImmutableList.Builder<ColumnReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
//        for (Field field : scope.getRelationType().getVisibleFields()) {
//            outputVariablesBuilder.add(variableAllocator.newVariable(field));
//        }
//
//        ImmutableList.Builder<List<RowExpression>> rowsBuilder = ImmutableList.builder();
//        for (Expression row : node.getRows()) {
//            ImmutableList.Builder<RowExpression> values = ImmutableList.builder();
//            if (row instanceof Row) {
//                for (Expression item : ((Row) row).getItems()) {
//                    values.add(rewriteRow(item));
//                }
//            }
//            else {
//                values.add(rewriteRow(row));
//            }
//            rowsBuilder.add(values.build());
//        }
//
//        ValuesNode valuesNode = new ValuesNode(idAllocator.getNextId(), outputVariablesBuilder.build(), rowsBuilder.build());
//        return new RelationPlan(valuesNode, scope, outputVariablesBuilder.build());
//    }
//
//    private RowExpression rewriteRow(Expression row)
//    {
//        // resolve enum literals
//        Expression expression = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
//        {
//            @Override
//            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
//            {
//                Type nodeType = analysis.getType(node);
//                Optional<Object> maybeEnumValue = tryResolveEnumLiteral(node, nodeType);
//                if (maybeEnumValue.isPresent()) {
//                    return new EnumLiteral(nodeType.getTypeSignature().toString(), maybeEnumValue.get());
//                }
//                return node;
//            }
//        }, row);
//        expression = Coercer.addCoercions(expression, analysis);
//        expression = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis.getParameters(), analysis), expression);
//        return castToRowExpression(expression);
//    }
//
//    @Override
//    public RelationPlan visitUnnest(Unnest node, Void context)
//    {
//        Scope scope = analysis.getScope(node);
//        ImmutableList.Builder<ColumnReferenceExpression> outputVariablesBuilder = ImmutableList.builder();
//        for (Field field : scope.getRelationType().getVisibleFields()) {
//            ColumnReferenceExpression variable = variableAllocator.newVariable(field);
//            outputVariablesBuilder.add(variable);
//        }
//        List<ColumnReferenceExpression> unnestedVariables = outputVariablesBuilder.build();
//
//        // If we got here, then we must be unnesting a constant, and not be in a join (where there could be column references)
//        ImmutableList.Builder<ColumnReferenceExpression> argumentVariables = ImmutableList.builder();
//        ImmutableList.Builder<RowExpression> values = ImmutableList.builder();
//        ImmutableMap.Builder<ColumnReferenceExpression, List<ColumnReferenceExpression>> unnestVariables = ImmutableMap.builder();
//        Iterator<ColumnReferenceExpression> unnestedVariablesIterator = unnestedVariables.iterator();
//        for (Expression expression : node.getExpressions()) {
//            Type type = analysis.getType(expression);
//            Expression rewritten = Coercer.addCoercions(expression, analysis);
//            rewritten = ExpressionTreeRewriter.rewriteWith(new ParameterRewriter(analysis.getParameters(), analysis), rewritten);
//            values.add(castToRowExpression(rewritten));
//            ColumnReferenceExpression input = variableAllocator.newVariable(rewritten, type);
//            argumentVariables.add(new ColumnReferenceExpression(input.getName(), type));
//            if (type instanceof ArrayType) {
//                Type elementType = ((ArrayType) type).getElementType();
//                if (!SystemSessionProperties.isLegacyUnnest(session) && elementType instanceof RowType) {
//                    ImmutableList.Builder<ColumnReferenceExpression> unnestVariableBuilder = ImmutableList.builder();
//                    for (int i = 0; i < ((RowType) elementType).getFields().size(); i++) {
//                        unnestVariableBuilder.add(unnestedVariablesIterator.next());
//                    }
//                    unnestVariables.put(input, unnestVariableBuilder.build());
//                }
//                else {
//                    unnestVariables.put(input, ImmutableList.of(unnestedVariablesIterator.next()));
//                }
//            }
//            else if (type instanceof MapType) {
//                unnestVariables.put(input, ImmutableList.of(unnestedVariablesIterator.next(), unnestedVariablesIterator.next()));
//            }
//            else {
//                throw new IllegalArgumentException("Unsupported type for UNNEST: " + type);
//            }
//        }
//        Optional<ColumnReferenceExpression> ordinalityVariable = node.isWithOrdinality() ? Optional.of(unnestedVariablesIterator.next()) : Optional.empty();
//        checkState(!unnestedVariablesIterator.hasNext(), "Not all output variables were matched with input variables");
//        ValuesNode valuesNode = new ValuesNode(
//                idAllocator.getNextId(),
//                argumentVariables.build(), ImmutableList.of(values.build()));
//
//        UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), valuesNode, ImmutableList.of(), unnestVariables.build(), ordinalityVariable);
//        return new RelationPlan(unnestNode, scope, unnestedVariables);
//    }
//
//    private RelationPlan processAndCoerceIfNecessary(Relation node, Void context)
//    {
//        Type[] coerceToTypes = analysis.getRelationCoercion(node);
//
//        RelationPlan plan = this.process(node, context);
//
//        if (coerceToTypes == null) {
//            return plan;
//        }
//
//        return addCoercions(plan, coerceToTypes);
//    }
//
//    private RelationPlan addCoercions(RelationPlan plan, Type[] targetColumnTypes)
//    {
//        RelationType oldRelation = plan.getDescriptor();
//        List<ColumnReferenceExpression> oldVisibleVariables = oldRelation.getVisibleFields().stream()
//                .map(oldRelation::indexOf)
//                .map(plan.getFieldMappings()::get)
//                .collect(toImmutableList());
//        RelationType oldRelationWithVisibleFields = plan.getDescriptor().withOnlyVisibleFields();
//        verify(targetColumnTypes.length == oldVisibleVariables.size());
//        ImmutableList.Builder<ColumnReferenceExpression> newVariables = new ImmutableList.Builder<>();
//        Field[] newFields = new Field[targetColumnTypes.length];
//        Assignments.Builder assignments = Assignments.builder();
//        for (int i = 0; i < targetColumnTypes.length; i++) {
//            ColumnReferenceExpression inputVariable = oldVisibleVariables.get(i);
//            Type outputType = targetColumnTypes[i];
//            if (!outputType.equals(inputVariable.getType())) {
//                Expression cast = new Cast(new SymbolReference(inputVariable.getName()), outputType.getTypeSignature().toString());
//                ColumnReferenceExpression outputVariable = variableAllocator.newVariable(cast, outputType);
//                assignments.put(outputVariable, castToRowExpression(cast));
//                newVariables.add(outputVariable);
//            }
//            else {
//                SymbolReference symbolReference = new SymbolReference(inputVariable.getName());
//                ColumnReferenceExpression outputVariable = variableAllocator.newVariable(symbolReference, outputType);
//                assignments.put(outputVariable, castToRowExpression(symbolReference));
//                newVariables.add(outputVariable);
//            }
//            Field oldField = oldRelationWithVisibleFields.getFieldByIndex(i);
//            newFields[i] = new Field(
//                    oldField.getRelationAlias(),
//                    oldField.getName(),
//                    targetColumnTypes[i],
//                    oldField.isHidden(),
//                    oldField.getOriginTable(),
//                    oldField.getOriginColumnName(),
//                    oldField.isAliased());
//        }
//        ProjectNode projectNode = new ProjectNode(idAllocator.getNextId(), plan.getRoot(), assignments.build());
//        return new RelationPlan(projectNode, Scope.builder().withRelationType(RelationId.anonymous(), new RelationType(newFields)).build(), newVariables.build());
//    }
//
//    @Override
//    public RelationPlan visitUnion(Union node, Void context)
//    {
//        checkArgument(!node.getRelations().isEmpty(), "No relations specified for UNION");
//
//        SetOperationPlan setOperationPlan = process(node);
//
//        RowNode RowNode = new UnionNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getOutputVariables(), setOperationPlan.getVariableMapping());
//        if (node.isDistinct().orElse(true)) {
//            RowNode = distinct(RowNode);
//        }
//        return new RelationPlan(RowNode, analysis.getScope(node), RowNode.getOutputVariables());
//    }
//
//    @Override
//    public RelationPlan visitIntersect(Intersect node, Void context)
//    {
//        checkArgument(!node.getRelations().isEmpty(), "No relations specified for INTERSECT");
//
//        SetOperationPlan setOperationPlan = process(node);
//
//        RowNode RowNode = new IntersectNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getOutputVariables(), setOperationPlan.getVariableMapping());
//        return new RelationPlan(RowNode, analysis.getScope(node), RowNode.getOutputVariables());
//    }
//
//    @Override
//    public RelationPlan visitExcept(Except node, Void context)
//    {
//        checkArgument(!node.getRelations().isEmpty(), "No relations specified for EXCEPT");
//
//        SetOperationPlan setOperationPlan = process(node);
//
//        RowNode RowNode = new ExceptNode(idAllocator.getNextId(), setOperationPlan.getSources(), setOperationPlan.getOutputVariables(), setOperationPlan.getVariableMapping());
//        return new RelationPlan(RowNode, analysis.getScope(node), RowNode.getOutputVariables());
//    }
//
//    private SetOperationPlan process(SetOperation node)
//    {
//        List<ColumnReferenceExpression> outputs = null;
//        ImmutableList.Builder<RowNode> sources = ImmutableList.builder();
//        ImmutableListMultimap.Builder<ColumnReferenceExpression, ColumnReferenceExpression> variableMapping = ImmutableListMultimap.builder();
//
//        List<RelationPlan> subPlans = node.getRelations().stream()
//                .map(relation -> processAndCoerceIfNecessary(relation, null))
//                .collect(toImmutableList());
//
//        for (RelationPlan relationPlan : subPlans) {
//            List<ColumnReferenceExpression> childOutputVariables = relationPlan.getFieldMappings();
//            if (outputs == null) {
//                // Use the first Relation to derive output variable names
//                RelationType descriptor = relationPlan.getDescriptor();
//                ImmutableList.Builder<ColumnReferenceExpression> outputVariableBuilder = ImmutableList.builder();
//                for (Field field : descriptor.getVisibleFields()) {
//                    int fieldIndex = descriptor.indexOf(field);
//                    ColumnReferenceExpression variable = childOutputVariables.get(fieldIndex);
//                    outputVariableBuilder.add(variableAllocator.newVariable(variable));
//                }
//                outputs = outputVariableBuilder.build();
//            }
//
//            RelationType descriptor = relationPlan.getDescriptor();
//            checkArgument(descriptor.getVisibleFieldCount() == outputs.size(),
//                    "Expected relation to have %s variables but has %s variables",
//                    descriptor.getVisibleFieldCount(),
//                    outputs.size());
//
//            int fieldId = 0;
//            for (Field field : descriptor.getVisibleFields()) {
//                int fieldIndex = descriptor.indexOf(field);
//                variableMapping.put(outputs.get(fieldId), childOutputVariables.get(fieldIndex));
//                fieldId++;
//            }
//
//            sources.add(relationPlan.getRoot());
//        }
//
//        return new SetOperationPlan(sources.build(), variableMapping.build());
//    }
//
//    private PlanBuilder initializePlanBuilder(RelationPlan relationPlan)
//    {
//        TranslationMap translations = new TranslationMap(relationPlan, analysis, lambdaDeclarationToVariableMap);
//
//        // Make field->variable mapping from underlying relation plan available for translations
//        // This makes it possible to rewrite FieldOrExpressions that reference fields from the underlying tuple directly
//        translations.setFieldMappings(relationPlan.getFieldMappings());
//
//        return new PlanBuilder(translations, relationPlan.getRoot());
//    }
//
//    private RowNode distinct(RowNode node)
//    {
//        return new AggregationNode(idAllocator.getNextId(),
//                node,
//                ImmutableMap.of(),
//                singleGroupingSet(node.getOutputVariables()),
//                ImmutableList.of(),
//                AggregationNode.Step.SINGLE,
//                Optional.empty(),
//                Optional.empty());
//    }
//
//    private static class SetOperationPlan
//    {
//        private final List<RowNode> sources;
//        private final List<ColumnReferenceExpression> outputVariables;
//        private final Map<ColumnReferenceExpression, List<ColumnReferenceExpression>> variableMapping;
//
//        private SetOperationPlan(List<RowNode> sources, ListMultimap<ColumnReferenceExpression, ColumnReferenceExpression> variableMapping)
//        {
//            this.sources = sources;
//            this.outputVariables = ImmutableList.copyOf(variableMapping.keySet());
//            Map<ColumnReferenceExpression, List<ColumnReferenceExpression>> mapping = new LinkedHashMap<>();
//            variableMapping.asMap().forEach((key, value) -> {
//                checkState(value instanceof List, "variableMapping values should be of type List");
//                mapping.put(key, (List<ColumnReferenceExpression>) value);
//            });
//            this.variableMapping = mapping;
//        }
//
//        public List<RowNode> getSources()
//        {
//            return sources;
//        }
//
//        public List<ColumnReferenceExpression> getOutputVariables()
//        {
//            return outputVariables;
//        }
//
//        public Map<ColumnReferenceExpression, List<ColumnReferenceExpression>> getVariableMapping()
//        {
//            return variableMapping;
//        }
//    }
}
