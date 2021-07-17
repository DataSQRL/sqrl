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
package ai.dataeng.sqml.planner.optimizations;

import static com.google.common.base.Preconditions.checkArgument;

import ai.dataeng.sqml.plan.Assignments;
import ai.dataeng.sqml.relational.OriginalExpressionUtils;
import ai.dataeng.sqml.sql.tree.ExistsPredicate;
import ai.dataeng.sqml.sql.tree.Expression;
import ai.dataeng.sqml.sql.tree.InPredicate;
import ai.dataeng.sqml.sql.tree.QuantifiedComparisonExpression;

public final class ApplyNodeUtil {

  private ApplyNodeUtil() {
  }

  public static void verifySubquerySupported(Assignments assignments) {
    checkArgument(
        assignments.getExpressions().stream().map(OriginalExpressionUtils::castToExpression)
            .allMatch(ApplyNodeUtil::isSupportedSubqueryExpression),
        "Unexpected expression used for subquery expression");
  }

  public static boolean isSupportedSubqueryExpression(Expression expression) {
    // TODO: add RowExpression support
    return expression instanceof InPredicate ||
        expression instanceof ExistsPredicate ||
        expression instanceof QuantifiedComparisonExpression;
  }
}
