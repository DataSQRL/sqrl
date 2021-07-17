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

import static ai.dataeng.sqml.relational.OriginalExpressionUtils.castToRowExpression;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.plan.Assignments;
import ai.dataeng.sqml.plan.PlanNode;
import ai.dataeng.sqml.plan.ProjectNode;
import ai.dataeng.sqml.relation.VariableReferenceExpression;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;
import java.util.Map;

public class PlanBuilder {

  private final TranslationMap translations;
  private final PlanNode root;

  public PlanBuilder(TranslationMap translations, PlanNode root) {
    requireNonNull(translations, "translations is null");
    requireNonNull(root, "root is null");

    this.translations = translations;
    this.root = root;
  }

  public TranslationMap copyTranslations() {
    TranslationMap translations = new TranslationMap(getRelationPlan(), getAnalysis());
    translations.copyMappingsFrom(getTranslations());
    return translations;
  }

  private Analysis getAnalysis() {
    return translations.getAnalysis();
  }

  public PlanBuilder withNewRoot(PlanNode root) {
    return new PlanBuilder(translations, root);
  }

  public RelationPlan getRelationPlan() {
    return translations.getRelationPlan();
  }

  public PlanNode getRoot() {
    return root;
  }

  public boolean canTranslate(Expression expression) {
    return translations.containsSymbol(expression);
  }

  public VariableReferenceExpression translate(Expression expression) {
    return translations.get(expression);
  }

  public VariableReferenceExpression translateToVariable(Expression expression) {
    return translations.get(expression);
  }

  public Expression rewrite(Expression expression) {
    return translations.rewrite(expression);
  }

  public TranslationMap getTranslations() {
    return translations;
  }

  public PlanBuilder appendProjections(Iterable<Expression> expressions,
      PlanVariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator) {
    TranslationMap translations = copyTranslations();

    Assignments.Builder projections = Assignments.builder();

    // add an identity projection for underlying plan
    for (VariableReferenceExpression variable : getRoot().getOutputVariables()) {
      projections.put(variable, castToRowExpression(new SymbolReference(variable.getName())));
    }

    ImmutableMap.Builder<VariableReferenceExpression, Expression> newTranslations = ImmutableMap
        .builder();
    for (Expression expression : expressions) {
      VariableReferenceExpression variable = variableAllocator
          .newVariable(expression, getAnalysis().getTypeWithCoercions(expression));
      projections.put(variable, castToRowExpression(translations.rewrite(expression)));
      newTranslations.put(variable, expression);
    }
    // Now append the new translations into the TranslationMap
    for (Map.Entry<VariableReferenceExpression, Expression> entry : newTranslations.build()
        .entrySet()) {
      translations.put(entry.getValue(), entry.getKey());
    }

    return new PlanBuilder(translations,
        new ProjectNode(idAllocator.getNextId(), getRoot(), projections.build()));
  }
}
