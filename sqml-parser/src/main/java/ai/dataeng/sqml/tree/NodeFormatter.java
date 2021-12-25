package ai.dataeng.sqml.tree;

import ai.dataeng.sqml.tree.SortItem.Ordering;
import java.util.stream.Collectors;

public class NodeFormatter extends AstVisitor<String, Void> {
  private static final NodeFormatter nodeFormatter = new NodeFormatter();
  public static String accept(Node node) {
    return node.accept(nodeFormatter, null);
  }

  @Override
  public String visitNode(Node node, Void context) {
    return "{}";
  }

  @Override
  public String visitImportDefinition(ImportDefinition node, Void context) {
    return "IMPORT " + node.getQualifiedName() + ";";
  }

  @Override
  public String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
    return node.getLeft().accept(this, null) + " " + node.getOperator().getValue() + " " +
        node.getRight().accept(this, null);
  }

  @Override
  public String visitComparisonExpression(ComparisonExpression node, Void context) {
    return node.getLeft().accept(this, null) + " " + node.getOperator().getValue() + " " +
        node.getRight().accept(this, null);
  }

  @Override
  public String visitDoubleLiteral(DoubleLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitDecimalLiteral(DecimalLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitQuery(Query node, Void context) {
    return node.getQueryBody().accept(this, null) +
        node.getOrderBy().map(o->
            o.getSortItems().stream()
              .map(s->s.accept(this, null))
              .collect(Collectors.joining("\n")))
        .orElse("")
        + node.getLimit().map(l -> " LIMIT " + l.getValue()).orElse("");
  }

  @Override
  public String visitGenericLiteral(GenericLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitTimeLiteral(TimeLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitSelect(Select node, Void context) {
    return (node.isDistinct()? " DISTINCT " : "") +
        node.getDistinctOn().map(o->o.accept(this, null)).orElse("") +
        node.getSelectItems().stream()
        .map(s->s.accept(this, null))
        .collect(Collectors.joining(", "));
  }

  @Override
  public String visitOrderBy(OrderBy node, Void context) {
    return node.getSortItems().stream().map(s->s.accept(this, null)).collect(Collectors.joining(", "));
  }

  @Override
  public String visitQuerySpecification(QuerySpecification node, Void context) {
    return "SELECT " + node.getSelect().accept(this, null) +
        " FROM " + node.getFrom().accept(this, null) +
        node.getWhere().map(w-> " WHERE " +w.accept(this, null)).orElse("") +
        node.getGroupBy().map(g-> " GROUP BY " +g.accept(this, null)).orElse("") +
        node.getHaving().map(h-> " HAVING " +h.accept(this, null)).orElse("") +
        node.getOrderBy().map(o-> " ORDER BY " +o.accept(this, null)).orElse("") +
        node.getLimit().map(l-> " LIMIT " +l.getValue()).orElse("");
  }

  @Override
  public String visitUnion(Union node, Void context) {
    return node.getRelations().stream()
        .map(r->r.accept(this, null))
        .collect(Collectors.joining(
            node.isDistinct().map(d->d.booleanValue() ? " UNION " : " UNION ALL ").orElse(" UNION ")
        ));
  }

  @Override
  public String visitIntersect(Intersect node, Void context) {
    return node.getRelations().stream()
        .map(r->r.accept(this, null))
        .collect(Collectors.joining(
            node.isDistinct().map(d->d.booleanValue() ? " INTERSECT " : " INTERSECT ALL ").orElse(" INTERSECT ")
        ));
  }

  @Override
  public String visitExcept(Except node, Void context) {
    return node.getRelations().stream()
        .map(r->r.accept(this, null))
        .collect(Collectors.joining(
            node.isDistinct().map(d->d.booleanValue() ? " EXCEPT " : " EXCEPT ALL ").orElse(" EXCEPT ")
        ));
  }

  @Override
  public String visitTimestampLiteral(TimestampLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitBetweenPredicate(BetweenPredicate node, Void context) {
    return "BETWEEN " + node.getMin() + " AND " + node.getMax();
  }

  @Override
  public String visitWhenClause(WhenClause node, Void context) {
    return "WHEN " + node.getOperand().accept(this, null) + " THEN " + node.getResult().accept(this, null);
  }

  @Override
  public String visitIntervalLiteral(IntervalLiteral node, Void context) {
    return "INTERVAL " + node.getExpression().accept(this, context) + " " + node.getStartField().name();
  }

  @Override
  public String visitInPredicate(InPredicate node, Void context) {
    return " IN TBD "; //node.getValue().accept(this, null) +
       // " IN (" + node.getValueList().accept(this, null) + ")";
  }

  @Override
  public String visitFunctionCall(FunctionCall node, Void context) {
    if (node.getName().equals(QualifiedName.of("="))) {
      return String.format("%s %s %s",
          node.getArguments().get(0).accept(this, null),
          node.getName(),
          node.getArguments().get(1).accept(this, null));
    }

    return node.getName() + "(" + node.getArguments().stream().map(a->a.accept(this, null)).collect(
        Collectors.joining(", ")) + ")" + node.getOver().map(o->o.accept(this, context)).orElse("");
  }

  @Override
  public String visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
    return "CASE " + node.getWhenClauses().stream().map(w->w.accept(this, null)).collect(
        Collectors.joining("\n")) + node.getDefaultValue().map(e->" ELSE " + e.accept(this, null)).orElse("");
  }

  @Override
  public String visitEnumLiteral(EnumLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitInListExpression(InListExpression node, Void context) {
    return node.getValues().stream().map(i->i.accept(this, null)).collect(Collectors.joining(", "));
  }

  @Override
  public String visitDereferenceExpression(DereferenceExpression node, Void context) {
    return node.getBase().accept(this, context) + "." + node.getField();
  }

  @Override
  public String visitNullLiteral(NullLiteral node, Void context) {
    return "null";
  }

  @Override
  public String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
    return node.getSign().name() + node.getValue().accept(this, null);
  }

  @Override
  public String visitNotExpression(NotExpression node, Void context) {
    return "NOT " + node.getValue().accept(this, null);
  }

  @Override
  public String visitSingleColumn(SingleColumn node, Void context) {
    return node.getExpression().accept(this, null) +
        node.getAlias().map(a->" AS " + a.accept(this, null))
        .orElse("");
  }

  @Override
  public String visitAllColumns(AllColumns node, Void context) {
    return "*";
  }

  @Override
  public String visitLikePredicate(LikePredicate node, Void context) {
    return node.getValue().accept(this, null) + " LIKE " + node.getPattern().accept(this, null);
  }

  @Override
  public String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
    return node.getValue().accept(this, null) + "IS NOT NULL";
  }

  @Override
  public String visitIsNullPredicate(IsNullPredicate node, Void context) {
    return node.getValue().accept(this, null) + "IS NULL";
  }

  @Override
  public String visitArrayConstructor(ArrayConstructor node, Void context) {
    return "ARRAY<"+node.getValues().stream().map(t->t.accept(this, null)).collect(Collectors.joining(", ")) + ">";
  }

  @Override
  public String visitLongLiteral(LongLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitParameter(Parameter node, Void context) {
    return String.valueOf(node.getPosition());
  }

  @Override
  public String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
    return node.getLeft().accept(this, null) +
        node.getOperator().name() +
        node.getRight().accept(this, null);
  }

  @Override
  public String visitSubqueryExpression(SubqueryExpression node, Void context) {
    return "(" + node.getQuery().accept(this, null) + ")";
  }

  @Override
  public String visitSortItem(SortItem node, Void context) {
    return node.getSortKey().accept(this, null) + " " +
        (node.getOrdering() == Ordering.ASCENDING ? "ASC" : "DESC");
  }

  @Override
  public String visitTable(Table node, Void context) {
    return node.getNamePath().getDisplay();
  }

  @Override
  public String visitRow(Row node, Void context) {
    return "TBD";
  }

  @Override
  public String visitTableSubquery(TableSubquery node, Void context) {
    return "(" + node.getQuery().accept(this, null) +
       ")";
  }

  @Override
  public String visitAliasedRelation(AliasedRelation node, Void context) {
    return node.getRelation().accept(this, null) +
        " AS " + node.getAlias().accept(this, null);
  }

  @Override
  public String visitJoin(Join node, Void context) {
    return  " " + node.getLeft().accept(this, null) + " " +
        node.getType() + " JOIN " +
        node.getRight().accept(this, null) + " " +
        node.getCriteria().map(c-> " ON " + c.accept(this, null))
        .orElse("");
  }

  @Override
  public String visitExists(ExistsPredicate node, Void context) {
    return "EXISTS(" + node.getSubquery().accept(this, null) + ")";
  }

  @Override
  public String visitCast(Cast node, Void context) {
    return "CAST("+ node.getExpression().accept(this, null) +" AS " +node.getType()+")";
  }

  @Override
  public String visitAtTimeZone(AtTimeZone node, Void context) {
    return super.visitAtTimeZone(node, context);
  }

  @Override
  public String visitGroupBy(GroupBy node, Void context) {
    return node.getGroupingElement().accept(this, null);
  }

  @Override
  public String visitGroupingElement(GroupingElement node, Void context) {
    return node.getExpressions().stream().map(g->g.accept(this, null))
        .collect(Collectors.joining(" "));
  }

  @Override
  public String visitSimpleGroupBy(SimpleGroupBy node, Void context) {
    return node.getExpressions().stream().map(e->e.accept(this, null)).collect(Collectors.joining(" "));
  }

  @Override
  public String visitSymbolReference(SymbolReference node, Void context) {
    return node.getName();
  }

  @Override
  public String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node,
      Void context) {
    return "TBD";
  }

  @Override
  public String visitGroupingOperation(GroupingOperation node, Void context) {
    return node.getGroupingColumns().stream().map(g->g.accept(this, null))
        .collect(Collectors.joining(" "));
  }

  @Override
  public String visitScript(ScriptNode node, Void context) {
    return node.getStatements()
        .stream().map(s->s.accept(this, null))
        .collect(Collectors.joining("\n"));
  }

  @Override
  public String visitExpressionAssignment(ExpressionAssignment node, Void context) {
    return node.getExpression().accept(this, null);
  }

  @Override
  public String visitQueryAssignment(QueryAssignment node, Void context) {
    return node.getQuery().accept(this, null);
  }

  @Override
  public String visitIsEmpty(IsEmpty isEmpty, Void context) {
    return "IS " + (isEmpty.isNotEmpty() ? "" : "NOT ") + "EMPTY";
  }

  @Override
  public String visitStringLiteral(StringLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitBooleanLiteral(BooleanLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  public String visitIdentifier(Identifier node, Void context) {
    return node.getValue();
  }

  @Override
  public String visitInlineJoin(InlineJoin node, Void context) {
    return node.getJoin().accept(this, context) + node.getInverse().map(i->" INVERSE " + i.getValue()).orElse("");
  }

  @Override
  public String visitInlineJoinBody(InlineJoinBody node, Void context) {
    return "JOIN " +node.getTable() + node.getAlias().map(a->" AS " + a.accept(this, null)).orElse("")  +
        " ON " + node.getCriteria() +
        (node.getSortItems().isEmpty() ? "" :  " ORDER BY ") +
        node.getSortItems().stream().map(i-> i.accept(this, context)).collect(Collectors.joining(", ")) +
        node.getLimit().map(i->" LIMIT " + i).orElse("") +
        node.getInlineJoinBody().map(j->j.accept(this, context));
  }

  @Override
  public String visitExpression(Expression node, Void context) {
    return node.accept(this, context);
  }

  @Override
  public String visitSelectItem(SelectItem node, Void context) {
    throw new RuntimeException(String.format("Undefiend node in printer %s", node.getClass().getName()));
  }

  @Override
  public String visitWindow(Window window, Void context) {
    return
        " OVER (PARTITION BY " + window.getPartitionBy().stream()
            .map(e->e.accept(this, context))
            .collect(Collectors.joining(", ")) + ")";
  }

  @Override
  public String visitDistinctOn(DistinctOn node, Void context) {
    return String.format(" ON (%s) ", node.getOn().stream()
        .map(e->e.getValue()).collect(Collectors.joining(",")));
  }

  @Override
  public String visitFieldReference(FieldReference node, Void context) {
    return node.getFieldIndex() + "";
  }

  @Override
  public String visitJoinOn(JoinOn node, Void context) {
    return node.getExpression().accept(this, null);
  }
}
