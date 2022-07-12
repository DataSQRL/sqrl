package ai.datasqrl.parse.tree;

import ai.datasqrl.parse.tree.SortItem.Ordering;
import java.util.stream.Collectors;

public class NodeFormatter extends AstVisitor<String, Object> {

  private static final NodeFormatter nodeFormatter = new NodeFormatter();

  public static String accept(Node node) {
    return node.accept(nodeFormatter, null);
  }

  @Override
  public String visitNode(Node node, Object context) {
    if (node != null) {
      throw new RuntimeException("Could not format sql node: " + node.getClass());
    }
    throw new RuntimeException("Could not format sql node");
  }

  @Override
  public String visitImportDefinition(ImportDefinition node, Object context) {
    return "IMPORT " + node.getNamePath() +
        node.getTimestamp().map(s->" TIMESTAMP " + s.accept(this, context))
          .orElse("")+ ";";
  }

  @Override
  public String visitExportDefinition(ExportDefinition node, Object context) {
    return "EXPORT " + node.getTablePath() + " TO " + node.getSinkPath();
  }

  @Override
  public String visitCreateSubscription(CreateSubscription node, Object context) {
    return "CREATE SUBSCRIPTION " +node.getSubscriptionType()+ " AS " + node.getQuery().accept(this, context);
  }

  @Override
  public String visitJoinAssignment(JoinAssignment node, Object context) {
    return node.getNamePath() + " := " + node.getJoinDeclaration().accept(this, context);
  }

  @Override
  public String visitArithmeticBinary(ArithmeticBinaryExpression node, Object context) {
    return node.getLeft().accept(this, context) + " " + node.getOperator().getValue() + " " +
        node.getRight().accept(this, context);
  }

  @Override
  public String visitComparisonExpression(ComparisonExpression node, Object context) {
    return node.getLeft().accept(this, context) + " " + node.getOperator().getValue() + " " +
        node.getRight().accept(this, context);
  }

  @Override
  public String visitDoubleLiteral(DoubleLiteral node, Object context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitDecimalLiteral(DecimalLiteral node, Object context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitQuery(Query node, Object context) {
    return node.getQueryBody().accept(this, context) +
        node.getOrderBy().map(o ->
                o.getSortItems().stream()
                    .map(s -> s.accept(this, context))
                    .collect(Collectors.joining("\n")))
            .orElse("")
        + node.getLimit().map(l -> " LIMIT " + l.getValue()).orElse("");
  }

  @Override
  public String visitGenericLiteral(GenericLiteral node, Object context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitTimeLiteral(TimeLiteral node, Object context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitSelect(Select node, Object context) {
    return (node.isDistinct() ? " DISTINCT " : "") +
//        node.getDistinctOn().map(o->o.accept(this, context)).orElse("") +
        node.getSelectItems().stream()
            .map(s -> s.accept(this, context))
            .collect(Collectors.joining(", "));
  }

  @Override
  public String visitOrderBy(OrderBy node, Object context) {
    return node.getSortItems().stream().map(s -> s.accept(this, context))
        .collect(Collectors.joining(", "));
  }

  @Override
  public String visitQuerySpecification(QuerySpecification node, Object context) {
    return "SELECT " + node.getSelect().accept(this, context) +
        " FROM " + node.getFrom().accept(this, context) +
        node.getWhere().map(w -> " WHERE " + w.accept(this, context)).orElse("") +
        node.getGroupBy().map(g -> " GROUP BY " + g.accept(this, context)).orElse("") +
        node.getHaving().map(h -> " HAVING " + h.accept(this, context)).orElse("") +
        node.getOrderBy().map(o -> " ORDER BY " + o.accept(this, context)).orElse("") +
        node.getLimit().map(l -> " LIMIT " + l.getValue()).orElse("");
  }

  @Override
  public String visitUnion(Union node, Object context) {
    return node.getRelations().stream()
        .map(r -> r.accept(this, context))
        .collect(Collectors.joining(
            node.isDistinct().map(d -> d.booleanValue() ? " UNION " : " UNION ALL ")
                .orElse(" UNION ")
        ));
  }

  @Override
  public String visitIntersect(Intersect node, Object context) {
    return node.getRelations().stream()
        .map(r -> r.accept(this, context))
        .collect(Collectors.joining(
            node.isDistinct().map(d -> d.booleanValue() ? " INTERSECT " : " INTERSECT ALL ")
                .orElse(" INTERSECT ")
        ));
  }

  @Override
  public String visitExcept(Except node, Object context) {
    return node.getRelations().stream()
        .map(r -> r.accept(this, context))
        .collect(Collectors.joining(
            node.isDistinct().map(d -> d.booleanValue() ? " EXCEPT " : " EXCEPT ALL ")
                .orElse(" EXCEPT ")
        ));
  }

  @Override
  public String visitTimestampLiteral(TimestampLiteral node, Object context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitBetweenPredicate(BetweenPredicate node, Object context) {
    return "BETWEEN " + node.getMin() + " AND " + node.getMax();
  }

  @Override
  public String visitWhenClause(WhenClause node, Object context) {
    return "WHEN " + node.getOperand().accept(this, context) + " THEN " + node.getResult()
        .accept(this, context);
  }

  @Override
  public String visitIntervalLiteral(IntervalLiteral node, Object context) {
    return "INTERVAL " + node.getExpression().accept(this, context) + " " + node.getStartField()
        .name();
  }

  @Override
  public String visitInPredicate(InPredicate node, Object context) {
    return " IN TBD "; //node.getValue().accept(this, context) +
    // " IN (" + node.getValueList().accept(this, context) + ")";
  }

  @Override
  public String visitFunctionCall(FunctionCall node, Object context) {
    return node.getNamePath() + "(" + node.getArguments().stream().map(a -> a.accept(this, context))
        .collect(
            Collectors.joining(", ")) + ")" + node.getOver().map(o -> o.accept(this, context))
        .orElse("");
  }

  @Override
  public String visitSimpleCaseExpression(SimpleCaseExpression node, Object context) {
    return "CASE " + node.getWhenClauses().stream().map(w -> w.accept(this, context)).collect(
        Collectors.joining("\n")) + node.getDefaultValue().map(e -> " ELSE " + e.accept(this, context))
        .orElse("");
  }

  @Override
  public String visitEnumLiteral(EnumLiteral node, Object context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitInListExpression(InListExpression node, Object context) {
    return node.getValues().stream().map(i -> i.accept(this, context))
        .collect(Collectors.joining(", "));
  }

  @Override
  public String visitDereferenceExpression(DereferenceExpression node, Object context) {
    return node.getBase().accept(this, context) + "." + node.getField();
  }

  @Override
  public String visitNullLiteral(NullLiteral node, Object context) {
    return "null";
  }

  @Override
  public String visitArithmeticUnary(ArithmeticUnaryExpression node, Object context) {
    return node.getSign().name() + node.getValue().accept(this, context);
  }

  @Override
  public String visitNotExpression(NotExpression node, Object context) {
    return "NOT " + node.getValue().accept(this, context);
  }

  @Override
  public String visitSingleColumn(SingleColumn node, Object context) {
    return node.getExpression().accept(this, context) +
        node.getAlias().map(a -> " AS " + a.accept(this, context))
            .orElse("");
  }

  @Override
  public String visitAllColumns(AllColumns node, Object context) {
    return node.getPrefix().map(p -> p + ".").orElse("") + "*";
  }

  @Override
  public String visitLikePredicate(LikePredicate node, Object context) {
    return node.getValue().accept(this, context) + " LIKE " + node.getPattern().accept(this, context);
  }

  @Override
  public String visitIsNotNullPredicate(IsNotNullPredicate node, Object context) {
    return node.getValue().accept(this, context) + " IS NOT NULL";
  }

  @Override
  public String visitIsNullPredicate(IsNullPredicate node, Object context) {
    return node.getValue().accept(this, context) + " IS NULL";
  }

  @Override
  public String visitArrayConstructor(ArrayConstructor node, Object context) {
    return "ARRAY<" + node.getValues().stream().map(t -> t.accept(this, context))
        .collect(Collectors.joining(", ")) + ">";
  }

  @Override
  public String visitLongLiteral(LongLiteral node, Object context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitParameter(Parameter node, Object context) {
    return String.valueOf(node.getPosition());
  }

  @Override
  public String visitLogicalBinaryExpression(LogicalBinaryExpression node, Object context) {
    return node.getLeft().accept(this, context) +
        " " + node.getOperator().name() + " " +
        node.getRight().accept(this, context);
  }

  @Override
  public String visitSubqueryExpression(SubqueryExpression node, Object context) {
    return "(" + node.getQuery().accept(this, context) + ")";
  }

  @Override
  public String visitSortItem(SortItem node, Object context) {
    return node.getSortKey().accept(this, context) +
        (node.getOrdering().map(o -> o == Ordering.ASCENDING ? " ASC" : " DESC").orElse(""));
  }

  @Override
  public String visitTableNode(TableNode node, Object context) {
    return node.getNamePath() + node.getAlias().map(a -> " AS " + a).orElse("");
  }

  @Override
  public String visitRow(Row node, Object context) {
    return "TBD";
  }

  @Override
  public String visitTableSubquery(TableSubquery node, Object context) {
    return "(" + node.getQuery().accept(this, context) +
        ")";
  }

  @Override
  public String visitJoin(Join node, Object context) {
    return " " + node.getLeft().accept(this, context) + " " +
        node.getType() + " JOIN " +
        node.getRight().accept(this, context) +
        node.getCriteria().map(c -> " ON " + c.accept(this, context))
            .orElse("");
  }

  @Override
  public String visitExists(ExistsPredicate node, Object context) {
    return "EXISTS(" + node.getSubquery().accept(this, context) + ")";
  }

  @Override
  public String visitCast(Cast node, Object context) {
    return "CAST(" + node.getExpression().accept(this, context) + " AS " + node.getType() + ")";
  }

  @Override
  public String visitAtTimeZone(AtTimeZone node, Object context) {
    return super.visitAtTimeZone(node, context);
  }

  @Override
  public String visitGroupBy(GroupBy node, Object context) {
    return node.getGroupingElement().accept(this, context);
  }

  @Override
  public String visitGroupingElement(GroupingElement node, Object context) {
    return node.getExpressions().stream().map(g -> g.accept(this, context))
        .collect(Collectors.joining(" "));
  }

  @Override
  public String visitSimpleGroupBy(SimpleGroupBy node, Object context) {
    return node.getExpressions().stream().map(e -> e.accept(this, context))
        .collect(Collectors.joining(" "));
  }

  @Override
  public String visitSymbolReference(SymbolReference node, Object context) {
    return node.getName();
  }

  @Override
  public String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node,
      Object context) {
    return "TBD";
  }

  @Override
  public String visitGroupingOperation(GroupingOperation node, Object context) {
    return node.getGroupingColumns().stream().map(g -> g.accept(this, context))
        .collect(Collectors.joining(" "));
  }

  @Override
  public String visitScript(ScriptNode node, Object context) {
    return node.getStatements()
        .stream().map(s -> s.accept(this, context))
        .collect(Collectors.joining("\n"));
  }

  @Override
  public String visitExpressionAssignment(ExpressionAssignment node, Object context) {
    return node.getNamePath() + " := " + node.getExpression().accept(this, context);
  }

  @Override
  public String visitQueryAssignment(QueryAssignment node, Object context) {
    return node.getNamePath() + " := " + node.getQuery().accept(this, context);
  }

  @Override
  public String visitIsEmpty(IsEmpty isEmpty, Object context) {
    return "IS " + (isEmpty.isNotEmpty() ? "" : "NOT ") + "EMPTY";
  }

  @Override
  public String visitStringLiteral(StringLiteral node, Object context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitBooleanLiteral(BooleanLiteral node, Object context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitIdentifier(Identifier node, Object context) {
    return node.getNamePath().toString();
  }

  @Override
  public String visitJoinDeclaration(JoinDeclaration node, Object context) {
    return node.getRelation().accept(this, context) +
        (node.getOrderBy().isEmpty() ? "" : " ORDER BY ") +
        node.getLimit().map(i -> " LIMIT " + i).orElse("") +
        node.getInverse().map(i -> " INVERSE " + i.getCanonical()).orElse("");
  }

  @Override
  public String visitExpression(Expression node, Object context) {
    return "{}";
  }

  @Override
  public String visitSelectItem(SelectItem node, Object context) {
    throw new RuntimeException(
        String.format("Undefiend node in printer %s", node.getClass().getName()));
  }

  @Override
  public String visitWindow(Window window, Object context) {
    return
        " OVER (PARTITION BY " + window.getPartitionBy().stream()
            .map(e -> e.accept(this, context))
            .collect(Collectors.joining(", ")) + ")";
  }

  @Override
  public String visitDistinctOn(DistinctOn node, Object context) {
    return String.format(" ON (%s) ", node.getOn().stream()
        .map(e -> e.getNamePath().toString()).collect(Collectors.joining(",")));
  }

  @Override
  public String visitFieldReference(FieldReference node, Object context) {
    return node.getFieldIndex() + "";
  }

  @Override
  public String visitJoinOn(JoinOn node, Object context) {
    return node.getExpression().accept(this, context);
  }

  @Override
  public String visitDistinctAssignment(DistinctAssignment node, Object context) {
    return node.getNamePath() + " := " + String.format("DISTINCT %s ON %s %s;", node.getTable(), node.getPartitionKeys(),
        node.getOrder() != null ? String.format("ORDER BY %s", node.getOrder()) : "");
  }

  @Override
  public String visitLimitNode(Limit node, Object context) {
    return node.getValue();
  }
}
