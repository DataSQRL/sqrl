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
package ai.dataeng.sqml.tree;

public class ExpressionRewriter<C> {

  public Expression rewriteExpression(Expression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return null;
  }

  public Expression rewriteRow(Row node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteArithmeticUnary(ArithmeticUnaryExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteArithmeticBinary(ArithmeticBinaryExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteComparisonExpression(ComparisonExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteBetweenPredicate(BetweenPredicate node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteLogicalBinaryExpression(LogicalBinaryExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteNotExpression(NotExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteIsNullPredicate(IsNullPredicate node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteIsNotNullPredicate(IsNotNullPredicate node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteSimpleCaseExpression(SimpleCaseExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteWhenClause(WhenClause node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteInListExpression(InListExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteFunctionCall(FunctionCall node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteLikePredicate(LikePredicate node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteInPredicate(InPredicate node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteExists(ExistsPredicate node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteSubqueryExpression(SubqueryExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteLiteral(Literal node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteArrayConstructor(ArrayConstructor node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteIdentifier(Identifier node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteDereferenceExpression(DereferenceExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteCast(Cast node, C context, ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteAtTimeZone(AtTimeZone node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteSymbolReference(SymbolReference node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteParameter(Parameter node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteQuantifiedComparison(QuantifiedComparisonExpression node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }

  public Expression rewriteGroupingOperation(GroupingOperation node, C context,
      ExpressionTreeRewriter<C> treeRewriter) {
    return rewriteExpression(node, context, treeRewriter);
  }
}
