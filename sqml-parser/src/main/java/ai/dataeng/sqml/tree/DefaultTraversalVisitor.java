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

public abstract class DefaultTraversalVisitor<R, C>
    extends AstVisitor<R, C> {


  @Override
  public R visitScript(ScriptNode scriptNode, C context) {
    for (Node item : scriptNode.getStatements()) {
      process(item, context);
    }
    return null;
  }

  @Override
  public R visitCast(Cast node, C context) {
    return process(node.getExpression(), context);
  }

  @Override
  public R visitArithmeticBinary(ArithmeticBinaryExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  public R visitBetweenPredicate(BetweenPredicate node, C context) {
    process(node.getValue(), context);
    process(node.getMin(), context);
    process(node.getMax(), context);

    return null;
  }

  @Override
  public R visitAtTimeZone(AtTimeZone node, C context) {
    process(node.getValue(), context);
    process(node.getTimeZone(), context);

    return null;
  }

  @Override
  public R visitArrayConstructor(ArrayConstructor node, C context) {
    for (Expression expression : node.getValues()) {
      process(expression, context);
    }

    return null;
  }

  @Override
  public R visitComparisonExpression(ComparisonExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  public R visitQuery(Query node, C context) {
    process(node.getQueryBody(), context);
    if (node.getOrderBy().isPresent()) {
      process(node.getOrderBy().get(), context);
    }

    return null;
  }

  @Override
  public R visitSelect(Select node, C context) {
    for (SelectItem item : node.getSelectItems()) {
      process(item, context);
    }

    return null;
  }

  @Override
  public R visitSingleColumn(SingleColumn node, C context) {
    process(node.getExpression(), context);

    return null;
  }

  @Override
  public R visitWhenClause(WhenClause node, C context) {
    process(node.getOperand(), context);
    process(node.getResult(), context);

    return null;
  }

  @Override
  public R visitInPredicate(InPredicate node, C context) {
    process(node.getValue(), context);
    //todo: IN may not be a list
    //process(node.getValueList(), context);

    return null;
  }

  @Override
  public R visitFunctionCall(FunctionCall node, C context) {
    for (Expression argument : node.getArguments()) {
      process(argument, context);
    }
//
//    if (node.getOrderBy().isPresent()) {
//      process(node.getOrderBy().get(), context);
//    }
//
//    if (node.getFilter().isPresent()) {
//      process(node.getFilter().get(), context);
//    }

    return null;
  }

  @Override
  public R visitGroupingOperation(GroupingOperation node, C context) {
    for (Expression columnArgument : node.getGroupingColumns()) {
      process(columnArgument, context);
    }

    return null;
  }

  @Override
  public R visitDereferenceExpression(DereferenceExpression node, C context) {
    process(node.getBase(), context);
    return null;
  }

  @Override
  public R visitSimpleCaseExpression(SimpleCaseExpression node, C context) {
//    process(node.getOperand(), context);
    for (WhenClause clause : node.getWhenClauses()) {
      process(clause, context);
    }

    node.getDefaultValue()
        .ifPresent(value -> process(value, context));

    return null;
  }

  @Override
  public R visitInListExpression(InListExpression node, C context) {
    for (Expression value : node.getValues()) {
      process(value, context);
    }

    return null;
  }

  @Override
  public R visitArithmeticUnary(ArithmeticUnaryExpression node, C context) {
    return process(node.getValue(), context);
  }

  @Override
  public R visitNotExpression(NotExpression node, C context) {
    return process(node.getValue(), context);
  }

  @Override
  public R visitLikePredicate(LikePredicate node, C context) {
    process(node.getValue(), context);
    process(node.getPattern(), context);

    return null;
  }

  @Override
  public R visitIsNotNullPredicate(IsNotNullPredicate node, C context) {
    return process(node.getValue(), context);
  }

  @Override
  public R visitIsNullPredicate(IsNullPredicate node, C context) {
    return process(node.getValue(), context);
  }

  @Override
  public R visitLogicalBinaryExpression(LogicalBinaryExpression node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    return null;
  }

  @Override
  public R visitSubqueryExpression(SubqueryExpression node, C context) {
    return process(node.getQuery(), context);
  }

  @Override
  public R visitOrderBy(OrderBy node, C context) {
    for (SortItem sortItem : node.getSortItems()) {
      process(sortItem, context);
    }
    return null;
  }

  @Override
  public R visitSortItem(SortItem node, C context) {
    return process(node.getSortKey(), context);
  }

  @Override
  public R visitQuerySpecification(QuerySpecification node, C context) {
    process(node.getSelect(), context);
    process(node.getFrom(), context);
    if (node.getWhere().isPresent()) {
      process(node.getWhere().get(), context);
    }
    if (node.getGroupBy().isPresent()) {
      process(node.getGroupBy().get(), context);
    }
    if (node.getHaving().isPresent()) {
      process(node.getHaving().get(), context);
    }
    if (node.getOrderBy().isPresent()) {
      process(node.getOrderBy().get(), context);
    }
    return null;
  }

  @Override
  public R visitSetOperation(SetOperation node, C context) {
    for (Relation relation : node.getRelations()) {
      process(relation, context);
    }
    return null;
  }

  @Override
  public R visitRow(Row node, C context) {
    for (Expression expression : node.getItems()) {
      process(expression, context);
    }
    return null;
  }

  @Override
  public R visitTableSubquery(TableSubquery node, C context) {
    return process(node.getQuery(), context);
  }

  @Override
  public R visitAliasedRelation(AliasedRelation node, C context) {
    return process(node.getRelation(), context);
  }

  @Override
  public R visitJoin(Join node, C context) {
    process(node.getLeft(), context);
    process(node.getRight(), context);

    node.getCriteria()
        .filter(criteria -> criteria instanceof JoinOn)
        .map(criteria -> process(((JoinOn) criteria).getExpression(), context));

    return null;
  }

  @Override
  public R visitGroupBy(GroupBy node, C context) {
    process(node.getGroupingElement(), context);

    return null;
  }

  @Override
  public R visitSimpleGroupBy(SimpleGroupBy node, C context) {
    for (Expression expression : node.getExpressions()) {
      process(expression, context);
    }

    return null;
  }

  @Override
  public R visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node, C context) {
    process(node.getValue(), context);
    process(node.getSubquery(), context);

    return null;
  }

  @Override
  public R visitExists(ExistsPredicate node, C context) {
    process(node.getSubquery(), context);

    return null;
  }

  @Override
  public R visitExpressionAssignment(ExpressionAssignment expressionAssignment, C context) {
    process(expressionAssignment.getExpression(), context);

    return null;
  }

  @Override
  public R visitQueryAssignment(QueryAssignment queryAssignment, C context) {
    process(queryAssignment.getQuery(), context);

    return null;
  }
}
