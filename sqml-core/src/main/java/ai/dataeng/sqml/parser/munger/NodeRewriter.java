package ai.dataeng.sqml.parser.munger;

import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.ArithmeticUnaryExpression;
import ai.dataeng.sqml.tree.ArrayConstructor;
import ai.dataeng.sqml.tree.AtTimeZone;
import ai.dataeng.sqml.tree.BetweenPredicate;
import ai.dataeng.sqml.tree.Cast;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.DereferenceExpression;
import ai.dataeng.sqml.tree.ExistsPredicate;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FieldReference;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.GroupingOperation;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.InListExpression;
import ai.dataeng.sqml.tree.InPredicate;
import ai.dataeng.sqml.tree.IsNotNullPredicate;
import ai.dataeng.sqml.tree.IsNullPredicate;
import ai.dataeng.sqml.tree.LikePredicate;
import ai.dataeng.sqml.tree.Literal;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NotExpression;
import ai.dataeng.sqml.tree.Parameter;
import ai.dataeng.sqml.tree.QuantifiedComparisonExpression;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Row;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SimpleCaseExpression;
import ai.dataeng.sqml.tree.SubqueryExpression;
import ai.dataeng.sqml.tree.SymbolReference;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.tree.WhenClause;

public class NodeRewriter<C> {

  public Node rewriteNode(Node node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return null;
  }

  public Expression rewriteExpression(Expression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return null;
  }

  public Node rewriteRow(Row node, C context, NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteArithmeticUnary(ArithmeticUnaryExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteArithmeticBinary(ArithmeticBinaryExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteComparisonExpression(ComparisonExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteBetweenPredicate(BetweenPredicate node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteLogicalBinaryExpression(LogicalBinaryExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteNotExpression(NotExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteIsNullPredicate(IsNullPredicate node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteIsNotNullPredicate(IsNotNullPredicate node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteSimpleCaseExpression(SimpleCaseExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteWhenClause(WhenClause node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteInListExpression(InListExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteFunctionCall(FunctionCall node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteLikePredicate(LikePredicate node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteInPredicate(InPredicate node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteExists(ExistsPredicate node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteSubqueryExpression(SubqueryExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteLiteral(Literal node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteArrayConstructor(ArrayConstructor node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteIdentifier(Identifier node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteDereferenceExpression(DereferenceExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteCast(Cast node, C context, NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteAtTimeZone(AtTimeZone node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteFieldReference(FieldReference node, C context, NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteSymbolReference(SymbolReference node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteParameter(Parameter node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteQuantifiedComparison(QuantifiedComparisonExpression node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteGroupingOperation(GroupingOperation node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteRelation(Relation node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteQuery(Query node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteQuerySpecification(QuerySpecification node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteTable(Table node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }
  public Node rewriteAliasedRelation(AliasedRelation node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }
  public Node rewriteSelect(Select node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }

  public Node rewriteGroupBy(GroupBy node, C context,
      NodeTreeRewriter<C> treeRewriter) {
    return rewriteNode(node, context, treeRewriter);
  }
}
