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
package ai.datasqrl.parse.tree;

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

  public R visitNode(Node node, C context) {
    return null;
  }

  //SQRL specific nodes

  public R visitScript(ScriptNode node, C context) {
    return visitNode(node, context);
  }

  public R visitImportDefinition(ImportDefinition node, C context) {
    return visitNode(node, context);
  }

  public R visitExportDefinition(ExportDefinition node, C context) {
    return visitNode(node, context);
  }

  public R visitAssignment(Assignment node, C context) {
    return visitNode(node, context);
  }

  public R visitDeclaration(Declaration declaration, C context) {
    return visitNode(declaration, context);
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

  public R visitCreateSubscription(CreateSubscription node, C context) {
    return visitNode(node, context);
  }

  public R visitDistinctAssignment(DistinctAssignment node, C context) {
    return visitAssignment(node, context);
  }

  public R visitJoinAssignment(JoinAssignment node, C context) {
    return visitAssignment(node, context);
  }

  public R visitJoinDeclaration(JoinDeclaration node, C context) {
    return visitDeclaration(node, context);
  }

  public R visitDereferenceExpression(DereferenceExpression node, C context) {
    return visitExpression(node, context);
  }

  // Query

  public R visitQuery(Query node, C context) {
    return visitStatement(node, context);
  }

  public R visitStatement(Statement node, C context) {
    return visitNode(node, context);
  }

  public R visitSelect(Select node, C context) {
    return visitNode(node, context);
  }

  public R visitRelation(Relation node, C context) {
    return visitNode(node, context);
  }

  public R visitQueryBody(QueryBody node, C context) {
    return visitRelation(node, context);
  }

  public R visitOrderBy(OrderBy node, C context) {
    return visitNode(node, context);
  }

  public R visitQuerySpecification(QuerySpecification node, C context) {
    return visitQueryBody(node, context);
  }

  public R visitSetOperation(SetOperation node, C context) {
    return visitQueryBody(node, context);
  }

  public R visitUnion(Union node, C context) {
    return visitSetOperation(node, context);
  }

  public R visitIntersect(Intersect node, C context) {
    return visitSetOperation(node, context);
  }

  public R visitExcept(Except node, C context) {
    return visitSetOperation(node, context);
  }

  public R visitWhenClause(WhenClause node, C context) {
    return visitExpression(node, context);
  }

  public R visitSelectItem(SelectItem node, C context) {
    return visitNode(node, context);
  }

  public R visitSingleColumn(SingleColumn node, C context) {
    return visitSelectItem(node, context);
  }

  public R visitAllColumns(AllColumns node, C context) {
    return visitSelectItem(node, context);
  }

  public R visitSortItem(SortItem node, C context) {
    return visitNode(node, context);
  }

  public R visitTableNode(TableNode node, C context) {
    return visitQueryBody(node, context);
  }

  public R visitRow(Row node, C context) {
    return visitNode(node, context);
  }

  public R visitTableSubquery(TableSubquery node, C context) {
    return visitQueryBody(node, context);
  }

  public R visitJoin(Join node, C context) {
    return visitRelation(node, context);
  }

  public R visitWindow(Window window, C context) {
    return visitNode(window, context);
  }

  public R visitDistinctOn(DistinctOn node, C context) {
    return visitNode(node, context);
  }

  public R visitJoinOn(JoinOn node, C context) {
    return visitNode(node, context);
  }

  public R visitExists(ExistsPredicate node, C context) {
    return visitExpression(node, context);
  }

  public R visitCast(Cast node, C context) {
    return visitExpression(node, context);
  }

  public R visitAtTimeZone(AtTimeZone node, C context) {
    return visitExpression(node, context);
  }

  public R visitGroupBy(GroupBy node, C context) {
    return visitNode(node, context);
  }

  public R visitGroupingElement(GroupingElement node, C context) {
    return visitNode(node, context);
  }

  public R visitSimpleGroupBy(SimpleGroupBy node, C context) {
    return visitGroupingElement(node, context);
  }

  public R visitGroupingOperation(GroupingOperation node, C context) {
    return visitExpression(node, context);
  }

  public R visitLimitNode(Limit node, C context) {
    return visitNode(node, context);
  }

  // Expresssions

  public R visitExpression(Expression node, C context) {
    return visitNode(node, context);
  }

  public R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  public R visitBetweenPredicate(BetweenPredicate node, C context) {
    return visitExpression(node, context);
  }

  public R visitComparisonExpression(ComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  public R visitInPredicate(InPredicate node, C context) {
    return visitExpression(node, context);
  }

  public R visitFunctionCall(FunctionCall node, C context) {
    return visitExpression(node, context);
  }

  public R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
    return visitExpression(node, context);
  }

  public R visitInListExpression(InListExpression node, C context) {
    return visitExpression(node, context);
  }

  public R visitIdentifier(Identifier node, C context) {
    return visitExpression(node, context);
  }

  public R visitLikePredicate(LikePredicate node, C context) {
    return visitExpression(node, context);
  }

  public R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  public R visitIsNullPredicate(IsNullPredicate node, C context) {
    return visitExpression(node, context);
  }

  public R visitNotExpression(NotExpression node, C context) {
    return visitExpression(node, context);
  }

  public R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    return visitExpression(node, context);
  }

  public R visitArrayConstructor(ArrayConstructor node, C context) {
    return visitExpression(node, context);
  }

  public R visitParameter(Parameter node, C context) {
    return visitExpression(node, context);
  }

  public R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
    return visitExpression(node, context);
  }

  public R visitSubqueryExpression(SubqueryExpression node, C context) {
    return visitExpression(node, context);
  }

  public R visitSymbolReference(SymbolReference node, C context) {
    return visitExpression(node, context);
  }

  public R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context) {
    return visitExpression(node, context);
  }

  public R visitFieldReference(FieldReference node, C context) {
    return visitExpression(node, context);
  }

  //Literals

  public R visitLiteral(Literal node, C context) {
    return visitExpression(node, context);
  }

  public R visitDoubleLiteral(DoubleLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitDecimalLiteral(DecimalLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitGenericLiteral(GenericLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitNullLiteral(NullLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitTimeLiteral(TimeLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitTimestampLiteral(TimestampLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitIntervalLiteral(IntervalLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitStringLiteral(StringLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitBooleanLiteral(BooleanLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitEnumLiteral(EnumLiteral node, C context) {
    return visitLiteral(node, context);
  }

  public R visitLongLiteral(LongLiteral node, C context) {
    return visitLiteral(node, context);
  }
}
