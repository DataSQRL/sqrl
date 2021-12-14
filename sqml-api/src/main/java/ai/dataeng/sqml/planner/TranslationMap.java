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
import ai.dataeng.sqml.analyzer.ResolvedField;
import ai.dataeng.sqml.analyzer.StatementAnalysis;
import ai.dataeng.sqml.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.tree.DereferenceExpression;
import ai.dataeng.sqml.tree.EnumLiteral;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.ExpressionRewriter;
import ai.dataeng.sqml.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.tree.FieldReference;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.NodeRef;
import ai.dataeng.sqml.tree.Parameter;
import ai.dataeng.sqml.tree.SymbolReference;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.linq4j.tree.LambdaExpression;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Keeps track of fields and expressions and their mapping to symbols in the current plan
 */
class TranslationMap
{
    // all expressions are rewritten in terms of fields declared by this relation plan
    private final RelationPlan rewriteBase;
    private final StatementAnalysis analysis;

    // current mappings of underlying field -> symbol for translating direct field references
    private final ColumnReferenceExpression[] fieldVariables;

    // current mappings of sub-expressions -> symbol
    private final Map<Expression, ColumnReferenceExpression> expressionToVariables = new HashMap<>();
    private final Map<Expression, Expression> expressionToExpressions = new HashMap<>();

    public TranslationMap(RelationPlan rewriteBase, StatementAnalysis analysis)
    {
        this.rewriteBase = requireNonNull(rewriteBase, "rewriteBase is null");
        this.analysis = requireNonNull(analysis, "analysis is null");

        fieldVariables = new ColumnReferenceExpression[rewriteBase.getFieldMappings().size()];
    }

    public RelationPlan getRelationPlan()
    {
        return rewriteBase;
    }

    public StatementAnalysis getStatementAnalysis()
    {
        return analysis;
    }

    public void setFieldMappings(List<ColumnReferenceExpression> variables)
    {
        checkArgument(variables.size() == fieldVariables.length, "size of variables list (%s) doesn't match number of expected fields (%s)", variables.size(), fieldVariables.length);

        for (int i = 0; i < variables.size(); i++) {
            this.fieldVariables[i] = variables.get(i);
        }
    }

    public void copyMappingsFrom(TranslationMap other)
    {
        checkArgument(other.fieldVariables.length == fieldVariables.length,
                "number of fields in other (%s) doesn't match number of expected fields (%s)",
                other.fieldVariables.length,
                fieldVariables.length);

        expressionToVariables.putAll(other.expressionToVariables);
        expressionToExpressions.putAll(other.expressionToExpressions);
        System.arraycopy(other.fieldVariables, 0, fieldVariables, 0, other.fieldVariables.length);
    }

    public void putExpressionMappingsFrom(TranslationMap other)
    {
        expressionToVariables.putAll(other.expressionToVariables);
        expressionToExpressions.putAll(other.expressionToExpressions);
    }

    public Expression rewrite(Expression expression)
    {
        // first, translate names from sql-land references to plan symbols
        Expression mapped = translateNamesToSymbols(expression);

        // then rewrite subexpressions in terms of the current mappings
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (expressionToVariables.containsKey(node)) {
                    return new SymbolReference(expressionToVariables.get(node).getName());
                }

                Expression translated = expressionToExpressions.getOrDefault(node, node);
                return treeRewriter.defaultRewrite(translated, context);
            }
        }, mapped);
    }

    public void put(Expression expression, ColumnReferenceExpression variable)
    {
        if (expression instanceof FieldReference) {
            int fieldIndex = ((FieldReference) expression).getFieldIndex();
            fieldVariables[fieldIndex] = variable;
            expressionToVariables.put(new SymbolReference(rewriteBase.getVariable(fieldIndex).getName()), variable);
            return;
        }

        Expression translated = translateNamesToSymbols(expression);
        expressionToVariables.put(translated, variable);

        // also update the field mappings if this expression is a field reference
        //todo has field references?
//        rewriteBase.getScope().tryResolveField(expression)
//                .filter(ResolvedField::isLocal)
//                .ifPresent(field -> fieldVariables[field.getHierarchyFieldIndex()] = variable);
    }

    public boolean containsSymbol(Expression expression)
    {
        if (expression instanceof FieldReference) {
            int field = ((FieldReference) expression).getFieldIndex();
            return fieldVariables[field] != null;
        }

        Expression translated = translateNamesToSymbols(expression);
        return expressionToVariables.containsKey(translated);
    }

    public ColumnReferenceExpression get(Expression expression)
    {
        if (expression instanceof FieldReference) {
            int field = ((FieldReference) expression).getFieldIndex();
            checkArgument(fieldVariables[field] != null, "No mapping for field: %s", field);
            return fieldVariables[field];
        }

        Expression translated = translateNamesToSymbols(expression);
        if (!expressionToVariables.containsKey(translated)) {
            checkArgument(expressionToExpressions.containsKey(translated), "No mapping for expression: %s", expression);
            return get(expressionToExpressions.get(translated));
        }

        return expressionToVariables.get(translated);
    }

    public void put(Expression expression, Expression rewritten)
    {
        expressionToExpressions.put(translateNamesToSymbols(expression), rewritten);
    }

    private Expression translateNamesToSymbols(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);
                return coerceIfNecessary(node, rewrittenExpression);
            }
//
//            @Override
//            public Expression rewriteFieldReference(FieldReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
//            {
//                ColumnReferenceExpression variable = rewriteBase.getVariable(node.getFieldIndex());
//                checkState(variable != null, "No variable mapping for node '%s' (%s)", node, node.getFieldIndex());
//                return new SymbolReference(variable.getName());
//            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
//                LambdaArgumentDeclaration referencedLambdaArgumentDeclaration = analysis.getLambdaArgumentReference(node);
//                if (referencedLambdaArgumentDeclaration != null) {
//                    ColumnReferenceExpression variable = lambdaDeclarationToVariableMap.get(
//                        NodeRef.of(referencedLambdaArgumentDeclaration));
//                    return coerceIfNecessary(node, new SymbolReference(variable.getName()));
//                }
//                else {
                    return rewriteExpressionWithResolvedName(node);
//                }
            }

            private Expression rewriteExpressionWithResolvedName(Expression node)
            {
                return getVariable(rewriteBase, node)
                        .map(variable -> coerceIfNecessary(node, new SymbolReference(variable.getName())))
                        .orElse(coerceIfNecessary(node, node));
            }
//
//            @Override
//            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
//            {
//                if (analysis.isColumnReference(node)) {
//                    Optional<ResolvedField> resolvedField = rewriteBase.getScope().tryResolveField(node);
//                    if (resolvedField.isPresent()) {
//                        if (resolvedField.get().isLocal()) {
//                            return getVariable(rewriteBase, node)
//                                    .map(variable -> coerceIfNecessary(node, new SymbolReference(variable.getName())))
//                                    .orElseThrow(() -> new IllegalStateException("No symbol mapping for node " + node));
//                        }
//                    }
//                    // do not rewrite outer references, it will be handled in outer scope planner
//                    return node;
//                }
//
//                Type nodeType = analysis.getType(node);
//                Optional<Object> maybeEnumValue = tryResolveEnumLiteral(node, nodeType);
//                if (maybeEnumValue.isPresent()) {
//                    return new EnumLiteral(nodeType.getTypeSignature().toString(), maybeEnumValue.get());
//                }
//
//                return rewriteExpression(node, context, treeRewriter);
//            }
//
//            @Override
//            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
//            {
//                checkState(analysis.getCoercion(node) == null, "cannot coerce a lambda expression");
//
//                ImmutableList.Builder<LambdaArgumentDeclaration> newArguments = ImmutableList.builder();
//                for (LambdaArgumentDeclaration argument : node.getArguments()) {
//                    ColumnReferenceExpression variable = lambdaDeclarationToVariableMap.get(NodeRef.of(argument));
//                    newArguments.add(new LambdaArgumentDeclaration(new Identifier(variable.getName())));
//                }
//                Expression rewrittenBody = treeRewriter.rewrite(node.getBody(), null);
//                return new LambdaExpression(newArguments.build(), rewrittenBody);
//            }

            @Override
            public Expression rewriteParameter(Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                checkState(analysis.getParameters().size() > node.getPosition(), "Too few parameter values");
                return coerceIfNecessary(node, analysis.getParameters().get(node.getPosition()));
            }

            private Expression coerceIfNecessary(Expression original, Expression rewritten)
            {
//                Type coercion = analysis.getCoercion(original);
//                if (coercion != null) {
//                    rewritten = new Cast(
//                            rewritten,
//                            coercion.getTypeSignature().toString(),
//                            false,
//                            analysis.isTypeOnlyCoercion(original));
//                }
                return rewritten;
            }
        }, expression, null);
    }

    private Optional<ColumnReferenceExpression> getVariable(RelationPlan plan, Expression expression)
    {
        if (!analysis.isColumnReference(expression)) {
            // Expression can be a reference to lambda argument (or DereferenceExpression based on lambda argument reference).
            // In such case, the expression might still be resolvable with plan.getScope() but we should not resolve it.
            return Optional.empty();
        }
        return plan.getScope()
                .tryResolveField(expression)
//                .filter(ResolvedField::isLocal)
                .map(field -> requireNonNull(plan.getFieldMappings().get(field.getHierarchyFieldIndex())));
    }
}
