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

import javax.annotation.Nullable;

/**
 * Concrete elements delegate to its closest abstract parent
 */
public abstract class AstVisitor<R, C> {

  public R process(Node node) {
    return process(node, null);
  }

  public R process(Node node, @Nullable C context) {
    return node.accept(this, context);
  }

  protected R visitNode(Node node, C context) {
    return null;
  }

  protected R visitImport(Import node, C context) {
    return visitNode(node, context);
  }

  protected R visitAssign(Assign node, C context) {
    return visitNode(node, context);
  }

  protected R visitAssignment(Assignment node, C context) {
    return visitNode(node, context);
  }

  protected R visitExpression(Expression node, C context) {
    return visitNode(node, context);
  }

  protected R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitBetweenPredicate(BetweenPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitComparisonExpression(ComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLiteral(Literal node, C context) {
    return visitExpression(node, context);
  }

  protected R visitDoubleLiteral(DoubleLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitDecimalLiteral(DecimalLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitStatement(Statement node, C context) {
    return visitNode(node, context);
  }

  protected R visitQuery(Query node, C context) {
    return visitStatement(node, context);
  }

  protected R visitGenericLiteral(GenericLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitTimeLiteral(TimeLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitSelect(Select node, C context) {
    return visitNode(node, context);
  }

  protected R visitRelation(Relation node, C context) {
    return visitNode(node, context);
  }

  protected R visitQueryBody(QueryBody node, C context) {
    return visitRelation(node, context);
  }

  protected R visitOrderBy(OrderBy node, C context) {
    return visitNode(node, context);
  }

  protected R visitQuerySpecification(QuerySpecification node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitSetOperation(SetOperation node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitUnion(Union node, C context) {
    return visitSetOperation(node, context);
  }

  protected R visitIntersect(Intersect node, C context) {
    return visitSetOperation(node, context);
  }

  protected R visitExcept(Except node, C context) {
    return visitSetOperation(node, context);
  }

  protected R visitTimestampLiteral(TimestampLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitWhenClause(WhenClause node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIntervalLiteral(IntervalLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitInPredicate(InPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitFunctionCall(FunctionCall node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitStringLiteral(StringLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitBooleanLiteral(BooleanLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitEnumLiteral(EnumLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitInListExpression(InListExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIdentifier(Identifier node, C context) {
    return visitExpression(node, context);
  }

  protected R visitDereferenceExpression(DereferenceExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitNullLiteral(NullLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitNotExpression(NotExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSelectItem(SelectItem node, C context) {
    return visitNode(node, context);
  }

  protected R visitSingleColumn(SingleColumn node, C context) {
    return visitSelectItem(node, context);
  }

  protected R visitAllColumns(AllColumns node, C context) {
    return visitSelectItem(node, context);
  }

  protected R visitLikePredicate(LikePredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitIsNullPredicate(IsNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitArrayConstructor(ArrayConstructor node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLongLiteral(LongLiteral node, C context) {
    return visitLiteral(node, context);
  }

  protected R visitParameter(Parameter node, C context) {
    return visitExpression(node, context);
  }

  protected R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSubqueryExpression(SubqueryExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitSortItem(SortItem node, C context) {
    return visitNode(node, context);
  }

  protected R visitTable(Table node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitRow(Row node, C context) {
    return visitNode(node, context);
  }

  protected R visitTableSubquery(TableSubquery node, C context) {
    return visitQueryBody(node, context);
  }

  protected R visitAliasedRelation(AliasedRelation node, C context) {
    return visitRelation(node, context);
  }

  protected R visitJoin(Join node, C context) {
    return visitRelation(node, context);
  }

  protected R visitExists(ExistsPredicate node, C context) {
    return visitExpression(node, context);
  }

  protected R visitCast(Cast node, C context) {
    return visitExpression(node, context);
  }

  protected R visitAtTimeZone(AtTimeZone node, C context) {
    return visitExpression(node, context);
  }

  protected R visitGroupBy(GroupBy node, C context) {
    return visitNode(node, context);
  }

  protected R visitGroupingElement(GroupingElement node, C context) {
    return visitNode(node, context);
  }

  protected R visitSimpleGroupBy(SimpleGroupBy node, C context) {
    return visitGroupingElement(node, context);
  }

  protected R visitSymbolReference(SymbolReference node, C context) {
    return visitExpression(node, context);
  }

  protected R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  protected R visitGroupingOperation(GroupingOperation node, C context) {
    return visitExpression(node, context);
  }

  protected R visitScript(Script node, C context) {
    return visitNode(node, context);
  }

  public R visitExpressionAssignment(ExpressionAssignment node, C context) {
    return visitAssignment(node, context);
  }
  public R visitQueryAssignment(QueryAssignment node, C context) {
    return visitAssignment(node, context);
  }
  public R visitIsEmpty(IsEmpty node, C context) {
    return visitExpression(node, context);
  }
  public R visitInlineJoinExpression(InlineJoinExpression node, C context) {
    return visitExpression(node, context);
  }
  public R visitInlineJoin(InlineJoin node, C context) {
    return visitNode(node, context);
  }

  public R visitCreateSubscription(CreateSubscription node, C context) {
    return visitNode(node, context);
  }

  public R visitDistinctAssignment(DistinctAssignment node, C context) {
    return visitAssignment(node, context);
  }
}
