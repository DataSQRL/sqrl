package ai.datasqrl.parse.tree;

import static ai.datasqrl.parse.util.SqrlNodeUtil.and;

import ai.datasqrl.parse.tree.SortItem.Ordering;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceOrdinal;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedFunctionCall;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.RelationNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import java.util.Optional;
import java.util.stream.Collectors;

public class NodeFormatter extends AstVisitor<String, Object> {

  private static final NodeFormatter nodeFormatter = new NodeFormatter();

  public static String accept(Node node) {
    return node.accept(nodeFormatter, null);
  }

  @Override
  public String visitNode(Node node, Object context) {
    throw new RuntimeException("");
//    return "{}";
  }

  @Override
  public String visitImportDefinition(ImportDefinition node, Object context) {
    return "IMPORT " + node.getNamePath() +
        node.getTimestamp().map(s->" TIMESTAMP " + s.accept(this, context))
          .orElse("")+ ";";
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
  public String visitAliasedRelation(AliasedRelation node, Object context) {
    return node.getRelation().accept(this, context) +
        " AS " + node.getAlias().accept(this, context);
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
  public String visitJoinAssignment(JoinDeclaration node, Object context) {
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
//
//  @Override
//  public String visitQuerySpecNorm(QuerySpecNorm node, Object context) {
//    StringBuilder b = new StringBuilder();
//    b.append("SELECT ")
//        .append(node.isDistinct() ? " DISTINCT " : "");
//    b.append(node.getSelect().stream().map(e->e.accept(this, node))
//        .collect(Collectors.joining(", ")));
//    b.append(" FROM ")
//        .append(node.getFrom().accept(this, context));
//    node.getAddlJoins().stream()
//        .map(e->e.accept(this, context))
//        .forEach(b::append);
//    node.getWhere().map(w-> b.append(" WHERE " + w.accept(this, node)));
//    if (!node.getGroupBy().isEmpty()) {
//      b.append(" GROUP BY ");
//      b.append(String.join(",", node.getGroupBy().stream().map(i->i.accept(this, node))
//          .collect(Collectors.toList())));
//    }
//    node.getHaving().map(h->b.append(" HAVING " + h.accept(this, node)));
//
//    if (!node.getOrders().isEmpty()) {
//      b.append(" ORDER BY ");
//      b.append(String.join(",", node.getOrders().stream().map(i->i.accept(this, node))
//          .collect(Collectors.toList())));
//    }
//
//    node.getLimit().map(l->b.append(" LIMIT " + l));
//
//    return b.toString();
//  }
//
//  public String walkFrom(List<RelBody> fromRoots) {
//    StringBuilder b = new StringBuilder();
//    for (RelBody relBody : fromRoots) {
//      b.append(walk(relBody));
//    }
//    return b.toString();
//  }
//
//  public String walk(RelBody relBody) {
//    StringBuilder b = new StringBuilder();
//
//    if (relBody instanceof JoinNorm) {
//      JoinNorm joinNorm = (JoinNorm) relBody;
//      b.append(" (" +walk(joinNorm.getLeftmost()) + ") " + joinNorm.getJoinType() +
//          " JOIN (" + walk(joinNorm.getRightmost()) + ")");
//    } else if (relBody instanceof TableNodeBody) {
//      TableNodeBody tableNodeBody = (TableNodeBody) relBody;
//      b.append(tableNodeBody.getTableItem().getTable());
//    }
//
//    return b.toString();
//
//  }
//
//  private String walkFrom2(List<TableItem> fromRoots) {
//    StringBuilder b = new StringBuilder();
//
//    for (int i = 0; i < fromRoots.size(); i++) {
//      TableItem tableItem = fromRoots.get(i);
//      if (i != 0) {
//        b.append(tableItem.getJoinType() + " JOIN ");
//      }
//      b.append(tableItem.getTable().getId());
//      tableItem.getAliasHint().map(a->b.append(" AS " + a + " "));
//
//      walkRel(tableItem.getNext(), b);
//    }
//    return b.toString();
//  }
//
//  private void walkRel(List<RelItem> next, StringBuilder b) {
//    for (int i = 0; i < next.size(); i++) {
//      RelItem relItem = next.get(i);
//      b.append(relItem.getJoinType() + " JOIN ");
//      if (relItem instanceof SubQueryItem2) {
//        b.append("(" + ((SubQueryItem2)relItem).getTableBody().accept(this, null) + ")");
//      } else {
//        b.append(relItem.getId());
//      }
//      relItem.getAliasHint().map(a->b.append(" AS " + a + " "));
//
//      walkRel(relItem.getNext(), b);
//    }
//  }

  @Override
  public String visitResolvedColumn(ResolvedColumn node, Object context) {
    return node.getColumn().getId().getCanonical();
  }
//
//  @Override
//  public String visitResolvedFunctionCall(ResolvedFunctionCall node, Object context) {
//    return visitFunctionCall(node, context);
//  }
//
//  @Override
//  public String visitReferenceOrdinal(ReferenceOrdinal node, Object context) {
//    QuerySpecNorm querySpecNorm = (QuerySpecNorm) context;
//    return "<" + querySpecNorm.getSelect().get(node.getOrdinal()).accept(this, context) + " ("+node.getOrdinal()+")>";
//  }

  @Override
  public String visitReferenceExpression(ReferenceExpression node, Object context) {
    return "<" + node.getReferences().accept(this, context) + ">";
  }

  @Override
  public String visitRelationNorm(RelationNorm node, Object context) {
    return super.visitRelationNorm(node, context);
  }

  @Override
  public String visitTableNorm(TableNodeNorm node, Object context) {
    return node.getRef().getTable().getId() + node.getAlias().map(a->" AS " + a).orElse("");
  }

//  @Override
//  public String visitSubQueryNorm(SubQueryNorm node, Object context) {
//    return "( " + node.getQuerySpecNorm().accept(this, null) +")";
//  }

//  @Override
//  public String visitJoinNorm(JoinNorm node, Object context) {
//    return " (" + node.getLeft().accept(this, context) + ") " + node.getJoinType() + " JOIN "
//        + " ("+ node.getRight().accept(this, context) + ") "
//        + Optional.ofNullable(node.getCriteria()).map(e-> " ON " + e.accept(this, context)).orElse("");
//  }
}
