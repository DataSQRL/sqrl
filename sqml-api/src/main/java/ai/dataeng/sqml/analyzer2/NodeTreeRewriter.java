package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.ArithmeticUnaryExpression;
import ai.dataeng.sqml.tree.ArrayConstructor;
import ai.dataeng.sqml.tree.AstVisitor;
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
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.LikePredicate;
import ai.dataeng.sqml.tree.Limit;
import ai.dataeng.sqml.tree.Literal;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NotExpression;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Parameter;
import ai.dataeng.sqml.tree.QuantifiedComparisonExpression;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Row;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SimpleCaseExpression;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.SubqueryExpression;
import ai.dataeng.sqml.tree.SymbolReference;
import ai.dataeng.sqml.tree.Table;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.WhenClause;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

public class NodeTreeRewriter<C> {

  private final NodeRewriter<C> rewriter;
  
  private final AstVisitor<Node, NodeTreeRewriter.Context<C>> visitor;

  public NodeTreeRewriter(NodeRewriter<C> rewriter) {
    this.rewriter = rewriter;
    this.visitor = new NodeTreeRewriter.RewritingVisitor();
  }

  public static <C, T extends Node> T rewriteWith(NodeRewriter<C> rewriter, T node) {
    return new NodeTreeRewriter<>(rewriter).rewrite(node, null);
  }

  public static <C, T extends Node> T rewriteWith(NodeRewriter<C> rewriter, T node,
      C context) {
    return new NodeTreeRewriter<>(rewriter).rewrite(node, context);
  }

  private static <T> boolean sameElements(Optional<T> a, Optional<T> b) {
    if (!a.isPresent() && !b.isPresent()) {
      return true;
    } else if (a.isPresent() != b.isPresent()) {
      return false;
    }

    return a.get() == b.get();
  }

  @SuppressWarnings("ObjectEquality")
  private static <T> boolean sameElements(Iterable<? extends T> a, Iterable<? extends T> b) {
    if (Iterables.size(a) != Iterables.size(b)) {
      return false;
    }

    Iterator<? extends T> first = a.iterator();
    Iterator<? extends T> second = b.iterator();

    while (first.hasNext() && second.hasNext()) {
      if (first.next() != second.next()) {
        return false;
      }
    }

    return true;
  }

  private List<Node> rewrite(List<Node> items, NodeTreeRewriter.Context<C> context) {
    ImmutableList.Builder<Node> builder = ImmutableList.builder();
    for (Node Node : items) {
      builder.add(rewrite(Node, context.get()));
    }
    return builder.build();
  }

  private List<SelectItem> rewriteSelectItem(List<SelectItem> items, NodeTreeRewriter.Context<C> context) {
    ImmutableList.Builder<SelectItem> builder = ImmutableList.builder();
    for (SelectItem Node : items) {
      builder.add(rewrite(Node, context.get()));
    }
    return builder.build();
  }

  private List<SortItem> rewriteSortItem(List<SortItem> items, NodeTreeRewriter.Context<C> context) {
    ImmutableList.Builder<SortItem> builder = ImmutableList.builder();
    for (SortItem Node : items) {
      builder.add(rewrite(Node, context.get()));
    }
    return builder.build();
  }

  private List<Expression> rewriteExpr(List<Expression> items, NodeTreeRewriter.Context<C> context) {
    ImmutableList.Builder<Expression> builder = ImmutableList.builder();
    for (Expression expr : items) {
      builder.add(rewrite(expr, context.get()));
    }
    return builder.build();
  }

  @SuppressWarnings("unchecked")
  public <T extends Node> T rewrite(T node, C context) {
    return (T) visitor.process(node, new NodeTreeRewriter.Context<>(context, false));
  }

  /**
   * Invoke the default rewrite logic explicitly. Specifically, it skips the invocation of the
   * Node rewriter for the provided node.
   */
  @SuppressWarnings("unchecked")
  public <T extends Node> T defaultRewrite(T node, C context) {
    return (T) visitor.process(node, new NodeTreeRewriter.Context<>(context, true));
  }

  public static class Context<C> {

    private final boolean defaultRewrite;
    private final C context;

    private Context(C context, boolean defaultRewrite) {
      this.context = context;
      this.defaultRewrite = defaultRewrite;
    }

    public C get() {
      return context;
    }

    public boolean isDefaultRewrite() {
      return defaultRewrite;
    }
  }

  private class RewritingVisitor
      extends AstVisitor<Node, NodeTreeRewriter.Context<C>> {

    @Override
    public Node visitNode(Node node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteNode(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      throw new UnsupportedOperationException(
          "not yet implemented: " + getClass().getSimpleName() + " for " + node.getClass()
              .getName());
    }

    @Override
    public Node visitExpression(Expression node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteExpression(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      throw new UnsupportedOperationException(
          "not yet implemented: " + getClass().getSimpleName() + " for " + node.getClass()
              .getName());
    }

    @Override
    public Node visitRow(Row node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter.rewriteRow(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      List<Expression> items = rewriteExpr(node.getItems(), context);

      if (!sameElements(node.getItems(), items)) {
        return new Row(items);
      }

      return node;
    }

    @Override
    public Node visitArithmeticUnary(ArithmeticUnaryExpression node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteArithmeticUnary(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression child = rewrite(node.getValue(), context.get());
      if (child != node.getValue()) {
        return new ArithmeticUnaryExpression(node.getSign(), child);
      }

      return node;
    }

    @Override
    public Node visitArithmeticBinary(ArithmeticBinaryExpression node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteArithmeticBinary(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression left = rewrite(node.getLeft(), context.get());
      Expression right = rewrite(node.getRight(), context.get());

      if (left != node.getLeft() || right != node.getRight()) {
        return new ArithmeticBinaryExpression(node.getOperator(), left, right);
      }

      return node;
    }

    @Override
    public Node visitArrayConstructor(ArrayConstructor node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteArrayConstructor(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      List<Expression> values = rewriteExpr(node.getValues(), context);

      if (!sameElements(node.getValues(), values)) {
        return new ArrayConstructor(values);
      }

      return node;
    }

    @Override
    public Node visitAtTimeZone(AtTimeZone node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteAtTimeZone(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      Expression timeZone = rewrite(node.getTimeZone(), context.get());

      if (value != node.getValue() || timeZone != node.getTimeZone()) {
        return new AtTimeZone(value, timeZone);
      }

      return node;
    }
//
//    @Override
//    public Node visitSubscriptExpression(SubscriptExpression node, Context<C> context) {
//      if (!context.isDefaultRewrite()) {
//        Node result = rewriter
//            .rewriteSubscriptExpression(node, context.get(), NodeTreeRewriter.this);
//        if (result != null) {
//          return result;
//        }
//      }
//
//      Expression base = rewrite(node.getBase(), context.get());
//      Expression index = rewrite(node.getIndex(), context.get());
//
//      if (base != node.getBase() || index != node.getIndex()) {
//        return new SubscriptExpression(base, index);
//      }
//
//      return node;
//    }

    @Override
    public Node visitComparisonExpression(ComparisonExpression node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteComparisonExpression(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression left = rewrite(node.getLeft(), context.get());
      Expression right = rewrite(node.getRight(), context.get());

      if (left != node.getLeft() || right != node.getRight()) {
        return new ComparisonExpression(node.getOperator(), left, right);
      }

      return node;
    }

    @Override
    public Node visitBetweenPredicate(BetweenPredicate node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteBetweenPredicate(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      Expression min = rewrite(node.getMin(), context.get());
      Expression max = rewrite(node.getMax(), context.get());

      if (value != node.getValue() || min != node.getMin() || max != node.getMax()) {
        return new BetweenPredicate(value, min, max);
      }

      return node;
    }

    @Override
    public Node visitLogicalBinaryExpression(LogicalBinaryExpression node,
        NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteLogicalBinaryExpression(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression left = rewrite(node.getLeft(), context.get());
      Expression right = rewrite(node.getRight(), context.get());

      if (left != node.getLeft() || right != node.getRight()) {
        return new LogicalBinaryExpression(node.getOperator(), left, right);
      }

      return node;
    }

    @Override
    public Node visitNotExpression(NotExpression node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteNotExpression(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());

      if (value != node.getValue()) {
        return new NotExpression(value);
      }

      return node;
    }

    @Override
    public Node visitIsNullPredicate(IsNullPredicate node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteIsNullPredicate(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());

      if (value != node.getValue()) {
        return new IsNullPredicate(value);
      }

      return node;
    }

    @Override
    public Node visitIsNotNullPredicate(IsNotNullPredicate node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteIsNotNullPredicate(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());

      if (value != node.getValue()) {
        return new IsNotNullPredicate(value);
      }

      return node;
    }

    @Override
    public Node visitSimpleCaseExpression(SimpleCaseExpression node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteSimpleCaseExpression(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

//      Expression operand = rewrite(node.getOperand(), context.get());

      ImmutableList.Builder<WhenClause> builder = ImmutableList.builder();
      for (WhenClause expression : node.getWhenClauses()) {
        builder.add(rewrite(expression, context.get()));
      }

      Optional<Expression> defaultValue = node.getDefaultValue()
          .map(value -> rewrite(value, context.get()));

      if (
          !sameElements(node.getDefaultValue(), defaultValue) ||
              !sameElements(node.getWhenClauses(), builder.build())) {
        return new SimpleCaseExpression( builder.build(), defaultValue);
      }

      return node;
    }

    @Override
    public Node visitWhenClause(WhenClause node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteWhenClause(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression operand = rewrite(node.getOperand(), context.get());
      Node result = rewrite(node.getResult(), context.get());

      if (operand != node.getOperand() || result != node.getResult()) {
        return new WhenClause(operand, (Expression)result);
      }
      return node;
    }

    @Override
    public Node visitFunctionCall(FunctionCall node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteFunctionCall(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      List<Expression> arguments = rewriteExpr(node.getArguments(), context);

      if (!sameElements(node.getArguments(), arguments)) {
        return new FunctionCall(node.getName(), arguments,
            node.isDistinct());
      }
      return node;
    }

    // Since OrderBy contains list of SortItems, we want to process each SortItem's key, which is an expression
    private OrderBy rewriteOrderBy(OrderBy orderBy, NodeTreeRewriter.Context<C> context) {
      List<SortItem> rewrittenSortItems = rewriteSortItems(orderBy.getSortItems(), context);
      if (sameElements(orderBy.getSortItems(), rewrittenSortItems)) {
        return orderBy;
      }

      return new OrderBy(rewrittenSortItems);
    }

    private List<SortItem> rewriteSortItems(List<SortItem> sortItems, NodeTreeRewriter.Context<C> context) {
      ImmutableList.Builder<SortItem> rewrittenSortItems = ImmutableList.builder();
      for (SortItem sortItem : sortItems) {
        Expression sortKey = rewrite(sortItem.getSortKey(), context.get());
        if (sortItem.getSortKey() != sortKey) {
          rewrittenSortItems
              .add(new SortItem(sortKey, sortItem.getOrdering()));
        } else {
          rewrittenSortItems.add(sortItem);
        }
      }
      return rewrittenSortItems.build();
    }

    @Override
    public Node visitLikePredicate(LikePredicate node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteLikePredicate(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      Expression pattern = rewrite(node.getPattern(), context.get());

      if (value != node.getValue() || pattern != node.getPattern()) {
        return new LikePredicate(value, pattern);
      }

      return node;
    }

    @Override
    public Node visitInPredicate(InPredicate node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteInPredicate(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      Expression list = rewrite(node.getValueList(), context.get());

      if (node.getValue() != value || node.getValueList() != list) {
        return new InPredicate(value, list);
      }

      return node;
    }

    @Override
    public Node visitInListExpression(InListExpression node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteInListExpression(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      List<Expression> values = rewriteExpr(node.getValues(), context);

      if (!sameElements(node.getValues(), values)) {
        return new InListExpression(values);
      }

      return node;
    }

    @Override
    public Node visitExists(ExistsPredicate node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteExists(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression subquery = node.getSubquery();
      subquery = rewrite(subquery, context.get());

      if (subquery != node.getSubquery()) {
        return new ExistsPredicate(subquery);
      }

      return node;
    }

    @Override
    public Node visitSubqueryExpression(SubqueryExpression node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteSubqueryExpression(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      // No default rewrite for SubqueryExpression since we do not want to traverse subqueries
      return node;
    }

    @Override
    public Node visitLiteral(Literal node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteLiteral(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Node visitParameter(Parameter node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteParameter(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Node visitIdentifier(Identifier node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteIdentifier(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Node visitDereferenceExpression(DereferenceExpression node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteDereferenceExpression(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression base = rewrite(node.getBase(), context.get());
      if (base != node.getBase()) {
        return new DereferenceExpression(base, node.getField());
      }

      return node;
    }

    @Override
    public Node visitCast(Cast node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter.rewriteCast(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression expression = rewrite(node.getExpression(), context.get());

      if (node.getExpression() != expression) {
        return new Cast(expression, node.getType(), node.isSafe(), node.isTypeOnly());
      }

      return node;
    }

    @Override
    public Node visitFieldReference(FieldReference node, NodeTreeRewriter.Context<C> context)
    {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter.rewriteFieldReference(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Node visitSymbolReference(SymbolReference node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteSymbolReference(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Node visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node,
        NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteQuantifiedComparison(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Expression value = rewrite(node.getValue(), context.get());
      Expression subquery = rewrite(node.getSubquery(), context.get());

      if (node.getValue() != value || node.getSubquery() != subquery) {
        return new QuantifiedComparisonExpression(node.getOperator(), node.getQuantifier(), value,
            subquery);
      }

      return node;
    }

    @Override
    public Node visitGroupingOperation(GroupingOperation node, NodeTreeRewriter.Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteGroupingOperation(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      return node;
    }

    @Override
    public Node visitQuery(Query node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteQuery(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }
      QueryBody queryBody = rewrite(node.getQueryBody(), context.get());
      Optional<OrderBy> order = node.getOrderBy()
          .map(value -> rewrite(value, context.get()));
      Optional<Limit> limit = node.getLimit()
          .map(value -> rewrite(value, context.get()));

      //todo: Check if objects have changed
      return new Query(node.getLocation(), queryBody, order, limit);
    }

    @Override
    public Node visitQuerySpecification(QuerySpecification node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteQuerySpecification(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }

      Select select = rewrite(node.getSelect(), context.get());
      Relation from = rewrite(node.getFrom(), context.get());
      Optional<Expression> where = node.getWhere()
          .map(value -> rewrite(value, context.get()));
      Optional<GroupBy> groupBy = node.getGroupBy()
          .map(value -> rewrite(value, context.get()));
      Optional<Expression> having = node.getHaving()
          .map(value -> rewrite(value, context.get()));
      Optional<OrderBy> order = node.getOrderBy()
          .map(value -> rewrite(value, context.get()));
      Optional<Limit> limit = node.getLimit()
          .map(value -> rewrite(value, context.get()));


      if (node.getSelect() != select || node.getFrom() != from ||
          !sameElements(node.getWhere(),  where) || !sameElements(node.getGroupBy(),  groupBy) ||
          !sameElements(node.getHaving(), having) || !sameElements(node.getOrderBy(),  order) ||
          !sameElements(node.getLimit(),  limit)
      ) {
        return new QuerySpecification(
            node.getLocation(),
            select,
            from,
            where,
            groupBy,
            having,
            order,
            limit
        );
      }

      return node;
    }

    @Override
    public Node visitSelect(Select node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteSelect(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }
      return new Select(node.isDistinct(), rewriteSelectItem(node.getSelectItems(), context));
    }

    @Override
    public Node visitTable(Table node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteTable(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }
      return new Table(node.getNamePath());
    }

    @Override
    public Node visitOrderBy(OrderBy node, Context<C> context) {
      return new OrderBy(rewriteSortItem(node.getSortItems(), context));
    }

    @Override
    public Node visitSingleColumn(SingleColumn node, Context<C> context) {

      return new SingleColumn(rewrite(node.getExpression(), context.get()), node.getAlias());
    }

    @Override
    public Node visitAllColumns(AllColumns node, Context<C> context) {
      return new AllColumns(node.getPrefix());
    }

    @Override
    public Node visitSortItem(SortItem node, Context<C> context) {
      return new SortItem(rewrite(node.getSortKey(), context.get()), node.getOrdering());
    }

    @Override
    public Node visitTableSubquery(TableSubquery node, Context<C> context) {
      return super.visitTableSubquery(node, context);
    }

    @Override
    public Node visitAliasedRelation(AliasedRelation node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteAliasedRelation(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }
      return new AliasedRelation(rewrite(node.getRelation(), context.get()), node.getAlias());
    }

    @Override
    public Node visitGroupBy(GroupBy node, Context<C> context) {
      if (!context.isDefaultRewrite()) {
        Node result = rewriter
            .rewriteGroupBy(node, context.get(), NodeTreeRewriter.this);
        if (result != null) {
          return result;
        }
      }
      return new GroupBy(rewrite(node.getGroupingElement(), context.get()));
    }

    @Override
    public Node visitSimpleGroupBy(SimpleGroupBy node, Context<C> context) {
      return new SimpleGroupBy(rewriteExpr(node.getExpressions(), context));
    }

    @Override
    public Node visitLimitNode(Limit node, Context<C> context) {
      return super.visitLimitNode(node, context);
    }

    @Override
    public Node visitJoin(Join node, Context<C> context) {
      return new Join(node.getType(), rewrite(node.getLeft(), context.get()), rewrite(node.getRight(), context.get()),
          node.getCriteria());
    }

  }
}