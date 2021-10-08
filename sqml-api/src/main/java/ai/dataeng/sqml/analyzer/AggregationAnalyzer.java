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

import static ai.dataeng.sqml.analyzer.ExpressionTreeUtils.extractAggregateFunctions;
import static ai.dataeng.sqml.analyzer.ScopeReferenceExtractor.getReferencesToScope;
import static ai.dataeng.sqml.analyzer.ScopeReferenceExtractor.hasReferencesToScope;
import static ai.dataeng.sqml.analyzer.ScopeReferenceExtractor.isFieldFromScope;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.INVALID_PROCEDURE_ARGUMENTS;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.MUST_BE_AGGREGATE_OR_GROUP_BY;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.NESTED_AGGREGATION;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION;
import static ai.dataeng.sqml.analyzer.SemanticErrorCode.REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.metadata.Metadata;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.ArithmeticUnaryExpression;
import ai.dataeng.sqml.tree.ArrayConstructor;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.AtTimeZone;
import ai.dataeng.sqml.tree.BetweenPredicate;
import ai.dataeng.sqml.tree.Cast;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.DereferenceExpression;
import ai.dataeng.sqml.tree.ExistsPredicate;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupingOperation;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.InListExpression;
import ai.dataeng.sqml.tree.InPredicate;
import ai.dataeng.sqml.tree.IsNotNullPredicate;
import ai.dataeng.sqml.tree.IsNullPredicate;
import ai.dataeng.sqml.tree.LikePredicate;
import ai.dataeng.sqml.tree.Literal;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.NotExpression;
import ai.dataeng.sqml.tree.Parameter;
import ai.dataeng.sqml.tree.Row;
import ai.dataeng.sqml.tree.SimpleCaseExpression;
import ai.dataeng.sqml.tree.SubqueryExpression;
import ai.dataeng.sqml.tree.WhenClause;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Checks whether an expression is constant with respect to the group
 */
class AggregationAnalyzer
{
    // fields and expressions in the group by clause
    private final Set<FieldId> groupingFields;
    private final List<Expression> expressions;
    private final Multimap<NodeRef<Expression>, FieldId> columnReferences;

    private final Metadata metadata;
    private final StatementAnalysis analysis;

    private final Scope sourceScope;
    private final Optional<Scope> orderByScope;
//    private final WarningCollector warningCollector;
//    private final FunctionResolution functionResolution;

    public static void verifySourceAggregations(
        List<Expression> groupByExpressions,
        Scope sourceScope,
        Expression expression,
        Metadata metadata,
        StatementAnalysis analysis)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.empty(), metadata, analysis);
        analyzer.analyze(expression);
    }

    public static void verifyOrderByAggregations(
        List<Expression> groupByExpressions,
        Scope sourceScope,
        Scope orderByScope,
        Expression expression,
        Metadata metadata,
        StatementAnalysis analysis)
    {
        AggregationAnalyzer analyzer = new AggregationAnalyzer(groupByExpressions, sourceScope, Optional.of(orderByScope), metadata, analysis);
        analyzer.analyze(expression);
    }

    private AggregationAnalyzer(List<Expression> groupByExpressions, Scope sourceScope, Optional<Scope> orderByScope, Metadata metadata, StatementAnalysis analysis)
    {
        requireNonNull(groupByExpressions, "groupByExpressions is null");
        requireNonNull(sourceScope, "sourceScope is null");
        requireNonNull(orderByScope, "orderByScope is null");
        requireNonNull(metadata, "metadata is null");
        requireNonNull(analysis, "analysis is null");

        this.sourceScope = sourceScope;
        this.orderByScope = orderByScope;
        this.metadata = metadata;
        this.analysis = analysis;
        this.expressions = groupByExpressions.stream()
            .collect(toImmutableList());

        this.columnReferences = analysis.getColumnReferenceFields();

        this.groupingFields = groupByExpressions.stream()
            .map(NodeRef::of)
            .filter(columnReferences::containsKey)
            .map(columnReferences::get)
            .flatMap(Collection::stream)
            .collect(toImmutableSet());

        this.groupingFields.forEach(fieldId -> {
            checkState(isFieldFromScope(fieldId, sourceScope),
                "Grouping field %s should originate from %s", fieldId, "TODO");
        });
    }

    private void analyze(Expression expression)
    {
        Visitor visitor = new Visitor();
        if (!visitor.process(expression, null)) {
            throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, expression, "'%s' must be an aggregate expression or appear in GROUP BY clause", expression);
        }
    }

    /**
     * visitor returns true if all expressions are constant with respect to the group.
     */
    private class Visitor
        extends AstVisitor<Boolean, Void>
    {
        @Override
        protected Boolean visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("aggregation analysis not yet implemented for: " + node.getClass().getName());
        }

        @Override
        protected Boolean visitAtTimeZone(AtTimeZone node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitSubqueryExpression(SubqueryExpression node, Void context)
        {
            /*
             * Column reference can resolve to (a) some subquery's scope, (b) a projection (ORDER BY scope),
             * (c) source scope or (d) outer query scope (effectively a constant).
             * From AggregationAnalyzer's perspective, only case (c) needs verification.
             */
            getReferencesToScope(node, analysis, sourceScope)
                .filter(expression -> !isGroupingKey(expression))
                .findFirst()
                .ifPresent(expression -> {
                    throw new SemanticException(MUST_BE_AGGREGATE_OR_GROUP_BY, expression,
                        "Subquery uses '%s' which must appear in GROUP BY clause", expression);
                });

            return true;
        }

        @Override
        protected Boolean visitExists(ExistsPredicate node, Void context)
        {
            checkState(node.getSubquery() instanceof SubqueryExpression);
            return process(node.getSubquery(), context);
        }

        @Override
        protected Boolean visitArrayConstructor(ArrayConstructor node, Void context)
        {
            return node.getValues().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitCast(Cast node, Void context)
        {
            return process(node.getExpression(), context);
        }

        @Override
        protected Boolean visitBetweenPredicate(BetweenPredicate node, Void context)
        {
            return process(node.getMin(), context) &&
                process(node.getValue(), context) &&
                process(node.getMax(), context);
        }

        @Override
        protected Boolean visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitLiteral(Literal node, Void context)
        {
            return true;
        }

        @Override
        protected Boolean visitIsNotNullPredicate(IsNotNullPredicate node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitIsNullPredicate(IsNullPredicate node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitLikePredicate(LikePredicate node, Void context)
        {
            return process(node.getValue(), context) && process(node.getPattern(), context);
        }

        @Override
        protected Boolean visitInListExpression(InListExpression node, Void context)
        {
            return node.getValues().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitInPredicate(InPredicate node, Void context)
        {
            return process(node.getValue(), context) && process(node.getValueList(), context);
        }

        @Override
        protected Boolean visitFunctionCall(FunctionCall node, Void context)
        {
            if (metadata.getFunctionProvider().resolve(node.getName()).get().isAggregation()) {
                List<FunctionCall> aggregateFunctions = extractAggregateFunctions(node.getArguments(), metadata.getFunctionProvider());

                if (!aggregateFunctions.isEmpty()) {
                    throw new SemanticException(NESTED_AGGREGATION,
                        node,
                        "Cannot nest aggregations inside aggregation '%s': %s",
                        node.getName(),
                        aggregateFunctions);
                }

                // ensure that no output fields are referenced from ORDER BY clause
                if (orderByScope.isPresent()) {
                    node.getArguments().stream()
                        .forEach(argument -> verifyNoOrderByReferencesToOutputColumns(
                            argument,
                            REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_AGGREGATION,
                            "Invalid reference to output projection attribute from ORDER BY aggregation"));
                }

                return true;
            }

            return node.getArguments().stream().allMatch(expression -> process(expression, context));
        }

        @Override
        protected Boolean visitIdentifier(Identifier node, Void context)
        {
            return isGroupingKey(node);
        }

        @Override
        protected Boolean visitDereferenceExpression(DereferenceExpression node, Void context)
        {
            if (columnReferences.containsKey(NodeRef.<Expression>of(node))) {
                return isGroupingKey(node);
            }

            // Allow SELECT col1.f1 FROM table1 GROUP BY col1
            return process(node.getBase(), context);
        }

        private boolean isGroupingKey(Expression node)
        {
            FieldId fieldId = checkAndGetColumnReferenceField(node, columnReferences);

            if (orderByScope.isPresent() && isFieldFromScope(fieldId, orderByScope.get())) {
                return true;
            }

            return groupingFields.contains(fieldId);
        }
        public FieldId checkAndGetColumnReferenceField(Expression expression, Multimap<NodeRef<Expression>, FieldId> columnReferences)
        {
            checkState(columnReferences.containsKey(NodeRef.of(expression)), "Missing field reference for expression");
            checkState(columnReferences.get(NodeRef.of(expression)).size() == 1, "Multiple field references for expression");

            return columnReferences.get(NodeRef.of(expression)).iterator().next();
        }
        @Override
        protected Boolean visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitNotExpression(NotExpression node, Void context)
        {
            return process(node.getValue(), context);
        }

        @Override
        protected Boolean visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return process(node.getLeft(), context) && process(node.getRight(), context);
        }

        @Override
        protected Boolean visitSimpleCaseExpression(SimpleCaseExpression node, Void context)
        {
//            if (!process(node.getOperand(), context)) {
//                return false;
//            }

            for (WhenClause whenClause : node.getWhenClauses()) {
                if (!process(whenClause.getOperand(), context) || !process(whenClause.getResult(), context)) {
                    return false;
                }
            }

            if (node.getDefaultValue().isPresent() && !process(node.getDefaultValue().get(), context)) {
                return false;
            }

            return true;
        }

        @Override
        public Boolean visitRow(Row node, final Void context)
        {
            return node.getItems().stream()
                .allMatch(item -> process(item, context));
        }

        @Override
        public Boolean visitParameter(Parameter node, Void context)
        {
            List<Expression> parameters = analysis.getParameters();
            checkArgument(node.getPosition() < parameters.size(), "Invalid parameter number %s, max values is %s", node.getPosition(), parameters.size() - 1);
            return process(parameters.get(node.getPosition()), context);
        }

        public Boolean visitGroupingOperation(GroupingOperation node, Void context)
        {
            // ensure that no output fields are referenced from ORDER BY clause
            if (orderByScope.isPresent()) {
                node.getGroupingColumns().forEach(groupingColumn -> verifyNoOrderByReferencesToOutputColumns(
                    groupingColumn,
                    REFERENCE_TO_OUTPUT_ATTRIBUTE_WITHIN_ORDER_BY_GROUPING,
                    "Invalid reference to output of SELECT clause from grouping() expression in ORDER BY"));
            }

            Optional<Expression> argumentNotInGroupBy = node.getGroupingColumns().stream()
                .filter(argument -> !columnReferences.containsKey(NodeRef.of(argument)) || !isGroupingKey(argument))
                .findAny();
            if (argumentNotInGroupBy.isPresent()) {
                throw new SemanticException(
                    INVALID_PROCEDURE_ARGUMENTS,
                    node,
                    "The arguments to GROUPING() must be expressions referenced by the GROUP BY at the associated query level. Mismatch due to %s.",
                    argumentNotInGroupBy.get());
            }
            return true;
        }

        @Override
        public Boolean process(Node node, @Nullable Void context)
        {
            if (expressions.stream().anyMatch(node::equals)
                && (!orderByScope.isPresent() || !hasOrderByReferencesToOutputColumns(node))) {
                return true;
            }

            return super.process(node, context);
        }
    }

    private boolean hasOrderByReferencesToOutputColumns(Node node)
    {
        return hasReferencesToScope(node, analysis, orderByScope.get());
    }

    private void verifyNoOrderByReferencesToOutputColumns(Node node, SemanticErrorCode errorCode, String errorString)
    {
        getReferencesToScope(node, analysis, orderByScope.get())
            .findFirst()
            .ifPresent(expression -> {
                throw new SemanticException(errorCode, expression, errorString);
            });
    }
}
