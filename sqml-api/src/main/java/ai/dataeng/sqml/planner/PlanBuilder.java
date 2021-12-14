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

import static ai.dataeng.sqml.planner.OriginalExpressionUtils.castToRowExpression;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.analyzer.StatementAnalysis;
import ai.dataeng.sqml.logical4.LogicalPlan.Node;
import ai.dataeng.sqml.logical4.LogicalPlan.RowNode;
import ai.dataeng.sqml.logical4.ProjectOperator;
import ai.dataeng.sqml.relation.ColumnReferenceExpression;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;
import java.util.Map;
import org.apache.calcite.interpreter.ProjectNode;

class PlanBuilder
{
    private final TranslationMap translations;
    private final Node root;

    public PlanBuilder(TranslationMap translations, Node root)
    {
        requireNonNull(translations, "translations is null");
        requireNonNull(root, "root is null");

        this.translations = translations;
        this.root = root;
    }

    public TranslationMap copyTranslations()
    {
        TranslationMap translations = new TranslationMap(getRelationPlan(), getStatementAnalysis());
        translations.copyMappingsFrom(getTranslations());
        return translations;
    }

    private StatementAnalysis getStatementAnalysis()
    {
        return translations.getStatementAnalysis();
    }

    public PlanBuilder withNewRoot(Node root)
    {
        return new PlanBuilder(translations, root);
    }

    public RelationPlan getRelationPlan()
    {
        return translations.getRelationPlan();
    }

    public Node getRoot()
    {
        return root;
    }

    public boolean canTranslate(Expression expression)
    {
        return translations.containsSymbol(expression);
    }

    public ColumnReferenceExpression translate(Expression expression)
    {
        return translations.get(expression);
    }

    public ColumnReferenceExpression translateToVariable(Expression expression)
    {
        return translations.get(expression);
    }

    public Expression rewrite(Expression expression)
    {
        return translations.rewrite(expression);
    }

    public TranslationMap getTranslations()
    {
        return translations;
    }
//
//    public PlanBuilder appendProjections(Iterable<Expression> expressions, PlanVariableAllocator variableAllocator, RowNodeIdAllocator idAllocator)
//    {
//        TranslationMap translations = copyTranslations();
//
//        Assignments.Builder projections = Assignments.builder();
//
//        // add an identity projection for underlying plan
//        for (ColumnReferenceExpression variable : getRoot().getOutputVariables()) {
//            projections.put(variable, castToRowExpression(new SymbolReference(variable.getName())));
//        }
//
//        ImmutableMap.Builder<ColumnReferenceExpression, Expression> newTranslations = ImmutableMap.builder();
//        for (Expression expression : expressions) {
//            ColumnReferenceExpression variable = variableAllocator.newVariable(expression, getStatementAnalysis().getTypeWithCoercions(expression));
//            projections.put(variable, castToRowExpression(translations.rewrite(expression)));
//            newTranslations.put(variable, expression);
//        }
//        // Now append the new translations into the TranslationMap
//        for (Map.Entry<ColumnReferenceExpression, Expression> entry : newTranslations.build().entrySet()) {
//            translations.put(entry.getValue(), entry.getKey());
//        }
//
//        return new PlanBuilder(translations, new ProjectOperator(idAllocator.getNextId(), getRoot(), projections.build()));
//    }
}
