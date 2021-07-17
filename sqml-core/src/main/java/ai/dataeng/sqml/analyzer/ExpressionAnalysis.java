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

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.sql.tree.ExistsPredicate;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.InPredicate;
import ai.dataeng.sqml.sql.tree.NodeRef;
import ai.dataeng.sqml.sql.tree.QuantifiedComparisonExpression;
import ai.dataeng.sqml.sql.tree.SubqueryExpression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.util.Map;
import java.util.Set;

public class ExpressionAnalysis {

  private final Map<NodeRef<Expression>, Type> expressionTypes;
  private final Set<NodeRef<InPredicate>> subqueryInPredicates;
  private final Set<NodeRef<SubqueryExpression>> scalarSubqueries;
  private final Set<NodeRef<ExistsPredicate>> existsSubqueries;
  private final Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons;

  public ExpressionAnalysis(
      Map<NodeRef<Expression>, Type> expressionTypes,
      Set<NodeRef<InPredicate>> subqueryInPredicates,
      Set<NodeRef<SubqueryExpression>> scalarSubqueries,
      Set<NodeRef<ExistsPredicate>> existsSubqueries,
      Set<NodeRef<QuantifiedComparisonExpression>> quantifiedComparisons) {
    this.expressionTypes = ImmutableMap
        .copyOf(requireNonNull(expressionTypes, "expressionTypes is null"));
    this.subqueryInPredicates = ImmutableSet
        .copyOf(requireNonNull(subqueryInPredicates, "subqueryInPredicates is null"));
    this.scalarSubqueries = ImmutableSet
        .copyOf(requireNonNull(scalarSubqueries, "subqueryInPredicates is null"));
    this.existsSubqueries = ImmutableSet
        .copyOf(requireNonNull(existsSubqueries, "existsSubqueries is null"));
    this.quantifiedComparisons = ImmutableSet
        .copyOf(requireNonNull(quantifiedComparisons, "quantifiedComparisons is null"));
  }

  public Type getType(Expression expression) {
    return expressionTypes.get(NodeRef.of(expression));
  }

  public Map<NodeRef<Expression>, Type> getExpressionTypes() {
    return expressionTypes;
  }

  public Set<NodeRef<InPredicate>> getSubqueryInPredicates() {
    return subqueryInPredicates;
  }

  public Set<NodeRef<SubqueryExpression>> getScalarSubqueries() {
    return scalarSubqueries;
  }

  public Set<NodeRef<ExistsPredicate>> getExistsSubqueries() {
    return existsSubqueries;
  }

  public Set<NodeRef<QuantifiedComparisonExpression>> getQuantifiedComparisons() {
    return quantifiedComparisons;
  }
}
