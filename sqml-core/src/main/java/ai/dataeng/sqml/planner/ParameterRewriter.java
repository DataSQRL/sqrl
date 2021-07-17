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

import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.analyzer.Analysis;
import ai.dataeng.sqml.common.type.Type;
import ai.dataeng.sqml.sql.tree.Cast;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.ExpressionRewriter;
import ai.dataeng.sqml.sql.tree.ExpressionTreeRewriter;
import ai.dataeng.sqml.sql.tree.Parameter;
import java.util.List;

public class ParameterRewriter
    extends ExpressionRewriter<Void> {

  private final List<Expression> parameterValues;
  private final Analysis analysis;

  public ParameterRewriter(List<Expression> parameterValues) {
    this(parameterValues, null);
  }

  public ParameterRewriter(List<Expression> parameterValues, Analysis analysis) {
    requireNonNull(parameterValues, "parameterValues is null");
    this.parameterValues = parameterValues;
    this.analysis = analysis;
  }

  @Override
  public Expression rewriteExpression(Expression node, Void context,
      ExpressionTreeRewriter<Void> treeRewriter) {
    return treeRewriter.defaultRewrite(node, context);
  }

  @Override
  public Expression rewriteParameter(Parameter node, Void context,
      ExpressionTreeRewriter<Void> treeRewriter) {
    checkState(parameterValues.size() > node.getPosition(), "Too few parameter values");
    return coerceIfNecessary(node, parameterValues.get(node.getPosition()));
  }

  private Expression coerceIfNecessary(Expression original, Expression rewritten) {
    if (analysis == null) {
      return rewritten;
    }

    Type coercion = analysis.getCoercion(original);
    if (coercion != null) {
      rewritten = new Cast(
          rewritten,
          coercion.getTypeSignature().toString(),
          false,
          analysis.isTypeOnlyCoercion(original));
    }
    return rewritten;
  }
}
