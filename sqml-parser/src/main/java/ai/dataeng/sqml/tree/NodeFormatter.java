package ai.dataeng.sqml.tree;

import ai.dataeng.sqml.parser.CreateRelationship;
import java.util.stream.Collectors;

public class NodeFormatter extends AstVisitor<String, Void> {
  private static final NodeFormatter nodeFormatter = new NodeFormatter();
  public static String accept(Node node) {
    return node.accept(nodeFormatter, null);
  }

  @Override
  protected String visitNode(Node node, Void context) {
    return "{}";
  }

  @Override
  protected String visitImport(Import node, Void context) {
    return "IMPORT " + node.getType().map(i->i + " ").orElse("")  + node.getQualifiedName() + ";";
  }

  @Override
  protected String visitAssign(Assign node, Void context) {
    return node.getName() + " := " + node.getRhs().accept(this, null) + ";";
  }

  @Override
  protected String visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
    return node.getLeft().accept(this, null) + " " + node.getOperator() + " " +
        node.getRight().accept(this, null);
  }

  @Override
  protected String visitComparisonExpression(ComparisonExpression node, Void context) {
    return node.getLeft().accept(this, null) + " " + node.getOperator() + " " +
        node.getRight().accept(this, null);
  }

  @Override
  protected String visitDoubleLiteral(DoubleLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitDecimalLiteral(DecimalLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitQuery(Query node, Void context) {
    return node.getQueryBody().accept(this, null) +
        node.getOrderBy().map(o->
            o.getSortItems().stream()
              .map(s->s.accept(this, null))
              .collect(Collectors.joining("\n")))
        .orElse("")
        + node.getLimit().map(l -> " LIMIT " + l).orElse("");
  }

  @Override
  protected String visitGenericLiteral(GenericLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitTimeLiteral(TimeLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitSelect(Select node, Void context) {
    return node.getSelectItems().stream()
        .map(s->s.accept(this, null))
        .collect(Collectors.joining(", "));
  }

  @Override
  protected String visitOrderBy(OrderBy node, Void context) {
    return node.getSortItems().stream().map(s->s.accept(this, null)).collect(Collectors.joining(", "));
  }

  @Override
  protected String visitQuerySpecification(QuerySpecification node, Void context) {
    return "SELECT " + node.getSelect().accept(this, null) +
        node.getFrom().map(f-> " FROM " +f.accept(this, null)).orElse("") +
        node.getWhere().map(w-> " WHERE " +w.accept(this, null)).orElse("") +
        node.getGroupBy().map(g-> " GROUP BY " +g.accept(this, null)).orElse("") +
        node.getHaving().map(h-> " HAVING " +h.accept(this, null)).orElse("") +
        node.getOrderBy().map(o-> " ORDER BY " +o.accept(this, null)).orElse("") +
        node.getLimit().map(l-> " LIMIT " +l).orElse("");
  }

  @Override
  protected String visitUnion(Union node, Void context) {
    return node.getRelations().stream()
        .map(r->r.accept(this, null))
        .collect(Collectors.joining(
            node.isDistinct().map(d->d.booleanValue() ? " UNION " : " UNION ALL ").orElse(" UNION ")
        ));
  }

  @Override
  protected String visitIntersect(Intersect node, Void context) {
    return node.getRelations().stream()
        .map(r->r.accept(this, null))
        .collect(Collectors.joining(
            node.isDistinct().map(d->d.booleanValue() ? " INTERSECT " : " INTERSECT ALL ").orElse(" INTERSECT ")
        ));
  }

  @Override
  protected String visitExcept(Except node, Void context) {
    return node.getRelations().stream()
        .map(r->r.accept(this, null))
        .collect(Collectors.joining(
            node.isDistinct().map(d->d.booleanValue() ? " EXCEPT " : " EXCEPT ALL ").orElse(" EXCEPT ")
        ));
  }

  @Override
  protected String visitTimestampLiteral(TimestampLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitBetweenPredicate(BetweenPredicate node, Void context) {
    return "BETWEEN " + node.getMin() + " AND " + node.getMax();
  }

  @Override
  protected String visitWhenClause(WhenClause node, Void context) {
    return "WHEN " + node.getOperand().accept(this, null) + " THEN " + node.getResult().accept(this, null);
  }

  @Override
  protected String visitIntervalLiteral(IntervalLiteral node, Void context) {
    return "INTERVAL " + String.valueOf(node.getValue()) + " " + node.getStartField().name();
  }

  @Override
  protected String visitInPredicate(InPredicate node, Void context) {
    return " IN TBD "; //node.getValue().accept(this, null) +
       // " IN (" + node.getValueList().accept(this, null) + ")";
  }

  @Override
  protected String visitFunctionCall(FunctionCall node, Void context) {
    return node.getName() + "(" + node.getArguments().stream().map(a->a.accept(this, null)).collect(
        Collectors.joining(", ")) + ")";
  }

  @Override
  protected String visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
    return "CASE " + node.getWhenClauses().stream().map(w->w.accept(this, null)).collect(
        Collectors.joining("\n")) + node.getDefaultValue().map(e->" ELSE " + e.accept(this, null)).orElse("");
  }

  @Override
  protected String visitEnumLiteral(EnumLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitInListExpression(InListExpression node, Void context) {
    return node.getValues().stream().map(i->i.accept(this, null)).collect(Collectors.joining(", "));
  }

  @Override
  protected String visitDereferenceExpression(DereferenceExpression node, Void context) {
    return "TBD";//super.visitDereferenceExpression(node, context);
  }

  @Override
  protected String visitNullLiteral(NullLiteral node, Void context) {
    return "null";
  }

  @Override
  protected String visitArithmeticUnary(ArithmeticUnaryExpression node, Void context) {
    return node.getSign().name() + node.getValue().accept(this, null);
  }

  @Override
  protected String visitNotExpression(NotExpression node, Void context) {
    return "NOT " + node.getValue().accept(this, null);
  }

  @Override
  protected String visitSingleColumn(SingleColumn node, Void context) {
    return node.getExpression().accept(this, null) +
        node.getAlias().map(a->" AS " + a.accept(this, null))
        .orElse("");
  }

  @Override
  protected String visitAllColumns(AllColumns node, Void context) {
    return "*";
  }

  @Override
  protected String visitLikePredicate(LikePredicate node, Void context) {
    return node.getValue().accept(this, null) + " LIKE " + node.getPattern().accept(this, null);
  }

  @Override
  protected String visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
    return node.getValue().accept(this, null) + "IS NOT NULL";
  }

  @Override
  protected String visitIsNullPredicate(IsNullPredicate node, Void context) {
    return node.getValue().accept(this, null) + "IS NULL";
  }

  @Override
  protected String visitArrayConstructor(ArrayConstructor node, Void context) {
    return "ARRAY<"+node.getValues().stream().map(t->t.accept(this, null)).collect(Collectors.joining(", ")) + ">";
  }

  @Override
  protected String visitLongLiteral(LongLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitParameter(Parameter node, Void context) {
    return String.valueOf(node.getPosition());
  }

  @Override
  protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
    return node.getLeft().accept(this, null) +
        node.getOperator().name() +
        node.getRight().accept(this, null);
  }

  @Override
  protected String visitSubqueryExpression(SubqueryExpression node, Void context) {
    return "(" + node.getQuery().accept(this, null) + ")";
  }

  @Override
  protected String visitSortItem(SortItem node, Void context) {
    return node.getSortKey().accept(this, null) + " " + node.getOrdering().name();
  }

  @Override
  protected String visitTable(Table node, Void context) {
    return node.getName().toString();
  }

  @Override
  protected String visitRow(Row node, Void context) {
    return "TBD";
  }

  @Override
  protected String visitTableSubquery(TableSubquery node, Void context) {
    return "(" + node.getQuery().accept(this, null) +
       ")";
  }

  @Override
  protected String visitAliasedRelation(AliasedRelation node, Void context) {
    return node.getRelation().accept(this, null) +
        " AS " + node.getAlias().accept(this, null) +
        "("+node.getColumnNames().stream().map(c->c.accept(this, null))
        .collect(Collectors.joining(","))+")";
  }

  @Override
  protected String visitJoin(Join node, Void context) {
    return node.getType() + " JOIN " +
        node.getLeft().accept(this, null) + " " +
        node.getRight().accept(this, null) + " " +
        node.getCriteria().map(c->c.accept(this, null))
        .orElse("");
  }

  @Override
  protected String visitExists(ExistsPredicate node, Void context) {
    return "EXISTS(" + node.getSubquery().accept(this, null) + ")";
  }

  @Override
  protected String visitCast(Cast node, Void context) {
    return "CAST("+ node.getExpression().accept(this, null) +" AS " +node.getType()+")";
  }

  @Override
  protected String visitAtTimeZone(AtTimeZone node, Void context) {
    return super.visitAtTimeZone(node, context);
  }

  @Override
  protected String visitGroupBy(GroupBy node, Void context) {
    return node.getGroupingElements().stream().map(g->g.accept(this, null))
        .collect(Collectors.joining(" "));
  }

  @Override
  protected String visitGroupingElement(GroupingElement node, Void context) {
    return node.getExpressions().stream().map(g->g.accept(this, null))
        .collect(Collectors.joining(" "));
  }

  @Override
  protected String visitSimpleGroupBy(SimpleGroupBy node, Void context) {
    return node.getExpressions().stream().map(e->e.accept(this, null)).collect(Collectors.joining(" "));
  }

  @Override
  protected String visitSymbolReference(SymbolReference node, Void context) {
    return node.getName();
  }

  @Override
  protected String visitQuantifiedComparisonExpression(QuantifiedComparisonExpression node,
      Void context) {
    return "TBD";
  }

  @Override
  protected String visitGroupingOperation(GroupingOperation node, Void context) {
    return node.getGroupingColumns().stream().map(g->g.accept(this, null))
        .collect(Collectors.joining(" "));
  }

  @Override
  protected String visitScript(Script node, Void context) {
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
  protected String visitStringLiteral(StringLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitBooleanLiteral(BooleanLiteral node, Void context) {
    return String.valueOf(node.getValue());
  }

  @Override
  protected String visitIdentifier(Identifier node, Void context) {
    return node.getValue();
  }

  @Override
  public String visitJoinSubexpression(JoinSubexpression node, Void context) {
    return node.getJoin().accept(this, null);
  }

  @Override
  public String visitInlineJoin(InlineJoin node, Void context) {
    return "JOIN " +node.getTable() + node.getAlias().map(a->" AS " + a.accept(this, null)).orElse("")  +
        " ON " + node.getCriteria() +
        node.getInverse().map(i->" INVERSE " + i).orElse("") +
        node.getLimit().map(i->" LIMIT " + i).orElse("");
  }

  @Override
  public String visitCreateRelationship(CreateRelationship node, Void context) {
    return node.getName() + " := JOIN " + node.getRelation() + " ON " + node.getExpression().accept(this, null)
         + node.getInverse().map(i->" INVERSE " + i).orElse("") +
        node.getLimit().map(l-> " LIMIT " + l).orElse("") + ";";
  }
}
