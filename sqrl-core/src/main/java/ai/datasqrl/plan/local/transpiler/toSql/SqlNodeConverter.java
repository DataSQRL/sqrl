package ai.datasqrl.plan.local.transpiler.toSql;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AS;

import ai.datasqrl.parse.tree.AliasedRelation;
import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.ArithmeticBinaryExpression;
import ai.datasqrl.parse.tree.ArrayConstructor;
import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.BetweenPredicate;
import ai.datasqrl.parse.tree.BooleanLiteral;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.DecimalLiteral;
import ai.datasqrl.parse.tree.DoubleLiteral;
import ai.datasqrl.parse.tree.Except;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.FunctionCall;
import ai.datasqrl.parse.tree.GenericLiteral;
import ai.datasqrl.parse.tree.GroupBy;
import ai.datasqrl.parse.tree.Hint;
import ai.datasqrl.parse.tree.Identifier;
import ai.datasqrl.parse.tree.InListExpression;
import ai.datasqrl.parse.tree.InPredicate;
import ai.datasqrl.parse.tree.Intersect;
import ai.datasqrl.parse.tree.IntervalLiteral;
import ai.datasqrl.parse.tree.IsNotNullPredicate;
import ai.datasqrl.parse.tree.IsNullPredicate;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.Limit;
import ai.datasqrl.parse.tree.Literal;
import ai.datasqrl.parse.tree.LogicalBinaryExpression;
import ai.datasqrl.parse.tree.LogicalBinaryExpression.Operator;
import ai.datasqrl.parse.tree.LongLiteral;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.NotExpression;
import ai.datasqrl.parse.tree.NullLiteral;
import ai.datasqrl.parse.tree.OrderBy;
import ai.datasqrl.parse.tree.Query;
import ai.datasqrl.parse.tree.QuerySpecification;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SetOperation;
import ai.datasqrl.parse.tree.SimpleCaseExpression;
import ai.datasqrl.parse.tree.SimpleGroupBy;
import ai.datasqrl.parse.tree.SingleColumn;
import ai.datasqrl.parse.tree.SortItem;
import ai.datasqrl.parse.tree.SortItem.Ordering;
import ai.datasqrl.parse.tree.StringLiteral;
import ai.datasqrl.parse.tree.SubqueryExpression;
import ai.datasqrl.parse.tree.TableNode;
import ai.datasqrl.parse.tree.TableSubquery;
import ai.datasqrl.parse.tree.TimeLiteral;
import ai.datasqrl.parse.tree.TimestampLiteral;
import ai.datasqrl.parse.tree.Union;
import ai.datasqrl.parse.tree.WhenClause;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqlParserPosFactory;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceExpression;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ReferenceOrdinal;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedColumn;
import ai.datasqrl.plan.local.transpiler.nodes.expression.ResolvedFunctionCall;
import ai.datasqrl.plan.local.transpiler.nodes.relation.JoinNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.QuerySpecNorm;
import ai.datasqrl.plan.local.transpiler.nodes.relation.TableNodeNorm;
import com.google.common.base.Preconditions;
import graphql.com.google.common.collect.ImmutableListMultimap;
import graphql.com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

/**
 * Converts sqrl's ast to calcite's ast
 */
public class SqlNodeConverter extends AstVisitor<SqlNode, ConvertContext> {

  private final ImmutableListMultimap<String, SqlOperator> opMap;
  SqlParserPosFactory pos = new SqlParserPosFactory();

  public SqlNodeConverter() {
    this.opMap = Multimaps.index(
        SqlStdOperatorTable.instance().getOperatorList(), e -> e.getName());
  }

  @Override
  public SqlNode visitNode(Node node, ConvertContext context) {
    throw new RuntimeException("Unrecognized node " + node + ":" + ((node != null) ? node.getClass().getName() : ""));
  }

  @Override
  public SqlNode visitQuerySpecNorm(QuerySpecNorm node, ConvertContext context) {
    ConvertContext convertContext = context.nested();
    SqlSelect select = new SqlSelect(
        pos.getPos(node.getLocation()),
        node.getSelect().isDistinct()
            ? new SqlNodeList(
            List.of(
                SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, pos.getPos(node.getLocation()))),
            pos.getPos(node.getLocation()))
            : null,
        toSelectList(node, convertContext),
        node.getFrom().accept(this, convertContext),
        node.getWhere().map(n -> n.accept(this, convertContext)).orElse(null),
        groupBy(node, convertContext),
        node.getHaving().map(n -> n.accept(this, context)).orElse(null),
        null,
        orderBy(node.getOrderBy(), convertContext),

        null,
        null,//node.getLimit().map(n -> n.accept(this, context)).orElse(null),
        null
    );
    if (context.isNested()) {
      return alias(select, context.getOrCreateAlias(node));
    } else {
      return select;
    }
  }

  private SqlNodeList orderBy(Optional<OrderBy> orderBy, ConvertContext convertContext) {
    if (orderBy.isEmpty()) return null;
    return (SqlNodeList)orderBy.get().accept(this, convertContext);
//    (SqlNodeList) new SqlNodeList(node.getOrders().stream().map(n -> n.accept(this, context)).collect(Collectors.toList()),
//        pos.getPos(Optional.empty())),

  }

  private SqlNodeList groupBy(QuerySpecNorm node, ConvertContext context) {
    if (!node.isAggregating()) {
      Preconditions.checkState(node.getGroupBy().isEmpty());
      return null;
    }

    List<SqlNode> selectList = new ArrayList<>();
    node.getParentPrimaryKeys().stream()
        .map(f->new SqlIdentifier(
            List.of(context.getOrCreateAlias(f.getRelationNorm()),
                f.getColumn().getId().getCanonical()), pos.getPos(Optional.empty())))
        .forEach(selectList::add);

    if (node.getGroupBy().isPresent()) {
      for (Expression f : node.getGroupBy().get().getGroupingElement().getExpressions()) {
        if (f instanceof ReferenceOrdinal) {
          ReferenceOrdinal referenceOrdinal = new ReferenceOrdinal(((ReferenceOrdinal)f).getOrdinal() +
              node.getParentPrimaryKeys().size() + node.getAddedPrimaryKeys().size());
          referenceOrdinal.setTable(((ReferenceOrdinal) f).getTable());
          selectList.add(referenceOrdinal.accept(this, context));
        } else {
          SqlNode accept = f.accept(this, context);
          selectList.add(accept);
        }
      }
    }
    if (selectList.isEmpty()) {
      return null;
    }

    return new SqlNodeList(selectList, pos.getPos(Optional.empty()));
  }

  private SqlNodeList toSelectList(QuerySpecNorm node, ConvertContext context) {
    List<SqlNode> selectList = new ArrayList<>();
    node.getParentPrimaryKeys().stream()
            .map(e->alias(e, e.getColumn().getId(), context))
            .forEach(selectList::add);

    node.getAddedPrimaryKeys().stream()
            .map(e->e.accept(this, context))
            .forEach(selectList::add);

    node.getSelect().getSelectItems().stream()
        .map(e->e.accept(this, context))
        .forEach(selectList::add);

    return new SqlNodeList(selectList, pos.getPos(Optional.empty()));
  }

  private SqlNode alias(Node e, Name name, ConvertContext context) {
    if (name == null) return e.accept(this, context);
    return alias(e, name.getCanonical(), context);
  }

  private SqlNode alias(Node e, String name, ConvertContext context) {
    return alias(e.accept(this, context), name);
  }

  private SqlNode alias(SqlNode sqlNode, String name) {
    if (name != null) {
      return new SqlBasicCall(AS,
          new SqlNode[]{sqlNode, new SqlIdentifier(name, pos.getPos(Optional.empty()))},
          pos.getPos(Optional.empty()));
    }
    return sqlNode;
  }

  @Override
  public SqlNode visitTableNorm(TableNodeNorm node, ConvertContext context) {

    return alias(
        new SqlTableRef(
            pos.getPos(Optional.empty()),
            new SqlIdentifier(node.getRef().getTable().getId().getCanonical(), pos.getPos(Optional.empty())),
            convertHints(node.getHints())),
        context.getOrCreateAlias(node));
  }

  private SqlNodeList convertHints(List<Hint> hints) {
    if (hints.isEmpty()) return SqlNodeList.EMPTY;
    return new SqlNodeList(hints.stream()
        .map(h-> convertHint(h))
        .collect(Collectors.toList()),
        pos.getPos(Optional.empty()));
  }

  private SqlHint convertHint(Hint h) {
    return new SqlHint(pos.getPos(Optional.empty()),
        new SqlIdentifier(h.getValue(), pos.getPos(Optional.empty())),
        SqlNodeList.EMPTY,
        HintOptionFormat.EMPTY);
  }

//  @Override
//  public SqlNode visitSubQueryNorm(SubQueryNorm node, ConvertContext context) {
//    return alias(node.getQuerySpecNorm(), context.getOrCreateAlias(node), context);
//  }

  @Override
  public SqlNode visitJoinNorm(JoinNorm node, ConvertContext context) {
    JoinType type;
    switch (node.getType()) {
      case DEFAULT:
      case TEMPORAL:
      case INNER:
        type = JoinType.INNER;
        break;
      case LEFT:
        type = JoinType.LEFT;
        break;
      case RIGHT:
        type = JoinType.RIGHT;
        break;
      case FULL:
        type = JoinType.FULL;
        break;
      case CROSS:
        type = JoinType.CROSS;
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + node.getType());
    }

//    SqlLiteral conditionType = node.getCriteria().map(c -> c instanceof JoinOn)
//        .map(on ->
//            SqlLiteral.createSymbol(on ? JoinConditionType.ON : JoinConditionType.NONE,
//                pos.getPos(node.getLocation())))
//        .orElse(SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO));

    return new SqlJoin(pos.getPos(node.getLocation()),
        node.getLeft().accept(this, context),
        SqlLiteral.createBoolean(false, pos.getPos(node.getLocation())),
        SqlLiteral.createSymbol(type, pos.getPos(node.getLocation())),
        node.getRight().accept(this, context),
        SqlLiteral.createSymbol(JoinConditionType.ON, pos.getPos(Optional.empty())),
        node.getCriteria().map(c->c.accept(this, context)).orElse(null)
    );
  }

  @Override
  public SqlNode visitResolvedColumn(ResolvedColumn node, ConvertContext context) {
    return new SqlIdentifier(List.of(
        context.getOrCreateAlias(node.getRelationNorm()),
        node.getColumn().getId().getCanonical()),
         pos.getPos(Optional.empty()));
  }

  //todo: remove and delegate entirely to calcite functions (via a sqrl to calcite bridge)
  @Override
  public SqlNode visitResolvedFunctionCall(ResolvedFunctionCall node, ConvertContext context) {
    String opName = node.getNamePath().getLast().getCanonical();
    List<SqlOperator> op = opMap.get(opName.toUpperCase());
    Preconditions.checkState(!op.isEmpty(), "Operation could not be found: %s", opName);

    SqlBasicCall call = new SqlBasicCall(op.get(0), toOperand(node.getArguments(), context),
        pos.getPos(node.getLocation()));

    //Convert to OVER
    if (op.get(0).requiresOver()) {
      List<SqlNode> partition = node.getOver().get().getPartitionBy().stream()
          .map(e -> e.accept(this, context)).collect(Collectors.toList());
      SqlNodeList orderList = SqlNodeList.EMPTY;
      if (node.getOver().get().getOrderBy().isPresent()) {
        OrderBy order = node.getOver().get().getOrderBy().get();
        List<SqlNode> ol = new ArrayList<>();
        for (SortItem sortItem : order.getSortItems()) {
          ol.add(sortItem.accept(this, context));
        }
        orderList = new SqlNodeList(ol, SqlParserPos.ZERO);
      }
      SqlNodeList partitionList = new SqlNodeList(partition, SqlParserPos.ZERO);

      SqlNode[] operands = {
          call,
          new SqlWindow(pos.getPos(node.getLocation()), null, null, partitionList, orderList,
              SqlLiteral.createBoolean(false, pos.getPos(node.getLocation())), null, null, null)
      };

      return new SqlBasicCall(SqlStdOperatorTable.OVER, operands, pos.getPos(node.getLocation()));
    }

    return call;
  }

  @Override
  public SqlNode visitReferenceOrdinal(ReferenceOrdinal node, ConvertContext context) {
    return
        SqlLiteral.createExactNumeric((node.getOrdinal()+1) + "",
        pos.getPos(Optional.empty()));
  }

  @Override
  public SqlNode visitReferenceExpression(ReferenceExpression node, ConvertContext context) {
    //Get the column name that it references...
    Name name = node.getRelationNorm().getFieldName(node.getReferences());

    return new SqlIdentifier(List.of(context.getOrCreateAlias(node.getRelationNorm()), name.getCanonical()),
        pos.getPos(Optional.empty()));
  }

  @Override
  public SqlNode visitQuery(Query node, ConvertContext context) {
    //todo: order w/ unions

    return node.getQueryBody().accept(this, context);
  }

  @Override
  public SqlNode visitSelect(Select node, ConvertContext context) {
    return new SqlNodeList(
        node.getSelectItems().stream().map(s -> s.accept(this, context))
            .collect(Collectors.toList()),
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitOrderBy(OrderBy node, ConvertContext context) {
    List<SqlNode> orderList = node.getSortItems().stream()
        .map(s -> s.accept(this, context))
        .collect(Collectors.toList());

    return new SqlNodeList(
        orderList,
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitQuerySpecification(QuerySpecification node, ConvertContext context) {
    SqlSelect select = new SqlSelect(
        pos.getPos(node.getLocation()),
        node.getSelect().isDistinct()
            ? new SqlNodeList(
            List.of(
                SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, pos.getPos(node.getLocation()))),
            pos.getPos(node.getLocation()))
            : null,
        (SqlNodeList) node.getSelect().accept(this, context),
        node.getFrom().accept(this, context),
        node.getWhere().map(n -> n.accept(this, context)).orElse(null),
        (SqlNodeList) node.getGroupBy().map(n -> n.accept(this, context)).orElse(null),
        node.getHaving().map(n -> n.accept(this, context)).orElse(null),
        null,
        (SqlNodeList) node.getOrderBy().map(n -> n.accept(this, context)).orElse(null),
        null,
        node.getLimit().map(n -> n.accept(this, context)).orElse(null),
        null
    );
    return select;
  }

  @Override
  public SqlNode visitUnion(Union node, ConvertContext context) {
    List<SqlNode> queries = node.getRelations().stream().map(x -> x.accept(this, context)).collect(Collectors.toList());
    SqlSetOperator op = node.isDistinct().orElse(false) ? SqlStdOperatorTable.UNION_ALL : SqlStdOperatorTable.UNION;
    return new SqlBasicCall(op, queries.toArray(new SqlNode[0]), SqlParserPosFactory.from(node));
  }


  @Override
  public SqlNode visitIntersect(Intersect node, ConvertContext context) {
    List<SqlNode> queries = node.getRelations().stream().map(x -> x.accept(this, context)).collect(Collectors.toList());
    SqlSetOperator op = node.isDistinct().orElse(false) ? SqlStdOperatorTable.INTERSECT_ALL : SqlStdOperatorTable.INTERSECT;
    return new SqlBasicCall(op, queries.toArray(new SqlNode[0]), SqlParserPosFactory.from(node));
  }

  @Override
  public SqlNode visitExcept(Except node, ConvertContext context) {
    List<SqlNode> queries = node.getRelations().stream().map(x -> x.accept(this, context)).collect(Collectors.toList());
    SqlSetOperator op = node.isDistinct().orElse(false) ? SqlStdOperatorTable.EXCEPT_ALL : SqlStdOperatorTable.EXCEPT;
    return new SqlBasicCall(op, queries.toArray(new SqlNode[0]), SqlParserPosFactory.from(node));
  }

  @Override
  public SqlNode visitSingleColumn(SingleColumn node, ConvertContext context) {
    SqlNode expr = node.getExpression().accept(this, context);
    if (node.getAlias().isPresent()) {
      return call(node.getLocation(),
          AS, context, node.getExpression(), node.getAlias().get());
    }
    return expr;
  }

  @Override
  public SqlNode visitAllColumns(AllColumns node, ConvertContext context) {
    return new SqlIdentifier("*", SqlParserPosFactory.from(node));
  }

  @Override
  public SqlNode visitSortItem(SortItem node, ConvertContext context) {
    SqlNode sortKey = node.getSortKey().accept(this, context);
    if (node.getOrdering().isPresent() && node.getOrdering().get() == Ordering.DESCENDING) {
      SqlNode[] sortOps = {sortKey};
      return new SqlBasicCall(SqlStdOperatorTable.DESC, sortOps, SqlParserPos.ZERO);
    }

    return sortKey;
  }

  @Override
  public SqlNode visitTableNode(TableNode node, ConvertContext context) {
    if (node.getAlias().isPresent()) {
      SqlIdentifier table = new SqlIdentifier(List.of(node.getNamePath().toString()),
          pos.getPos(node.getLocation()));
      SqlNode[] operands = {
          table,
          new SqlIdentifier(node.getAlias().get().getCanonical(), SqlParserPos.ZERO)
      };
      return new SqlBasicCall(AS, operands, pos.getPos(node.getLocation()));
    }

    return new SqlIdentifier(List.of(node.getNamePath().toString()),
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTableSubquery(TableSubquery node, ConvertContext context) {
    return node.getQuery().accept(this, context);
  }

  @Override
  public SqlNode visitAliasedRelation(AliasedRelation node, ConvertContext context) {
    SqlNode[] operand = {
        node.getRelation().accept(this, context),
        node.getAlias().accept(this, context)
    };

    return new SqlBasicCall(AS, operand, pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitJoin(Join node, ConvertContext context) {
    JoinType type;
    switch (node.getType()) {
      case INNER:
        type = JoinType.INNER;
        break;
      case LEFT:
        type = JoinType.LEFT;
        break;
      case RIGHT:
        type = JoinType.RIGHT;
        break;
      case FULL:
        type = JoinType.FULL;
        break;
      case CROSS:
        type = JoinType.CROSS;
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + node.getType());
    }

    SqlLiteral conditionType = node.getCriteria().map(c -> c instanceof JoinOn)
        .map(on ->
            SqlLiteral.createSymbol(on ? JoinConditionType.ON : JoinConditionType.NONE,
                pos.getPos(node.getLocation())))
        .orElse(SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO));

    return new SqlJoin(pos.getPos(node.getLocation()),
        node.getLeft().accept(this, context),
        SqlLiteral.createBoolean(false, pos.getPos(node.getLocation())),
        SqlLiteral.createSymbol(type, pos.getPos(node.getLocation())),
        node.getRight().accept(this, context),
        conditionType,
        node.getCriteria().map(e -> e.accept(this, context)).orElse(null)
    );
  }
//
//  @Override
//  public SqlNode visitWindow(Window window, ConvertContext context) {
//    return super.visitWindow(window, context);
//  }

  @Override
  public SqlNode visitJoinOn(JoinOn node, ConvertContext context) {
    return node.getExpression().accept(this, context);
  }
//
//  @Override
//  public SqlNode visitExists(ExistsPredicate node, ConvertContext context) {
//    throw new RuntimeException("ExistsPredicate Not yet implemented");
//  }
//
//  @Override
//  public SqlNode visitCast(Cast node, ConvertContext context) {
//    throw new RuntimeException("Cast Not yet implemented");
//  }
//
//  @Override
//  public SqlNode visitAtTimeZone(AtTimeZone node, ConvertContext context) {
//    throw new RuntimeException("AtTimeZone Not yet implemented");
//  }

  @Override
  public SqlNode visitGroupBy(GroupBy node, ConvertContext context) {
    return node.getGroupingElement().accept(this, context);
  }

  @Override
  public SqlNode visitSimpleGroupBy(SimpleGroupBy node, ConvertContext context) {
    return new SqlNodeList(
        node.getExpressions().stream().map(e -> e.accept(this, context)).collect(Collectors.toList()),
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitLimitNode(Limit node, ConvertContext context) {
    return node.getIntValue().map(i -> SqlLiteral.createExactNumeric(node.getValue(),
        pos.getPos(node.getLocation()))).orElse(null);
  }

  @Override
  public SqlNode visitArithmeticBinary(ArithmeticBinaryExpression node, ConvertContext context) {
    SqlOperator op;
    switch (node.getOperator()) {
      case ADD:
        op = SqlStdOperatorTable.PLUS;
        break;
      case SUBTRACT:
        op = SqlStdOperatorTable.MINUS;
        break;
      case MULTIPLY:
        op = SqlStdOperatorTable.MULTIPLY;
        break;
      case DIVIDE:
        op = SqlStdOperatorTable.DIVIDE;
        break;
      case MODULUS:
        op = SqlStdOperatorTable.MOD;
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + node.getOperator());
    }

    return call(node.getLocation(), op, context, node.getLeft(), node.getRight());
  }

  @Override
  public SqlNode visitBetweenPredicate(BetweenPredicate node, ConvertContext context) {
    return call(node.getLocation(), SqlStdOperatorTable.BETWEEN, context, node.getValue(), node.getMin(),
        node.getMax());
  }

  @Override
  public SqlNode visitComparisonExpression(ComparisonExpression node, ConvertContext context) {
    SqlOperator op;
    switch (node.getOperator()) {
      case EQUAL:
        op = SqlStdOperatorTable.EQUALS;
        break;
      case NOT_EQUAL:
        op = SqlStdOperatorTable.NOT_EQUALS;
        break;
      case LESS_THAN:
        op = SqlStdOperatorTable.LESS_THAN;
        break;
      case LESS_THAN_OR_EQUAL:
        op = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        break;
      case GREATER_THAN:
        op = SqlStdOperatorTable.GREATER_THAN;
        break;
      case GREATER_THAN_OR_EQUAL:
        op = SqlStdOperatorTable.GREATER_THAN_OR_EQUAL;
        break;
      default:
        throw new IllegalStateException("Unexpected value: " + node.getOperator());
    }

    return call(node.getLocation(), op, context, node.getLeft(), node.getRight());
  }

  @Override
  public SqlNode visitInPredicate(InPredicate node, ConvertContext context) {
    return call(node.getLocation(), SqlStdOperatorTable.IN, context, node.getValueList());
  }

  @Override
  public SqlNode visitFunctionCall(FunctionCall node, ConvertContext context) {
    String opName = node.getNamePath().get(0).getCanonical();
    List<SqlOperator> op = opMap.get(opName.toUpperCase());
    Preconditions.checkState(!op.isEmpty(), "Operation could not be found: %s", opName);

    SqlBasicCall call = new SqlBasicCall(op.get(0), toOperand(node.getArguments(), context),
        pos.getPos(node.getLocation()));

    //Convert to OVER
    if (op.get(0).requiresOver()) {
      List<SqlNode> partition = node.getOver().get().getPartitionBy().stream()
          .map(e -> e.accept(this, context)).collect(Collectors.toList());
      SqlNodeList orderList = SqlNodeList.EMPTY;
      if (node.getOver().get().getOrderBy().isPresent()) {
        OrderBy order = node.getOver().get().getOrderBy().get();
        List<SqlNode> ol = new ArrayList<>();
        for (SortItem sortItem : order.getSortItems()) {
          ol.add(sortItem.accept(this, context));
        }
        orderList = new SqlNodeList(ol, SqlParserPos.ZERO);
      }
      SqlNodeList partitionList = new SqlNodeList(partition, SqlParserPos.ZERO);

      SqlNode[] operands = {
          call,
          new SqlWindow(pos.getPos(node.getLocation()), null, null, partitionList, orderList,
              SqlLiteral.createBoolean(false, pos.getPos(node.getLocation())), null, null, null)
      };

      return new SqlBasicCall(SqlStdOperatorTable.OVER, operands, pos.getPos(node.getLocation()));
    }

    return call;
  }

  @Override
  public SqlNode visitSimpleCaseExpression(SimpleCaseExpression node, ConvertContext context) {
    List<SqlNode> whenList = new ArrayList<>();
    List<SqlNode> thenList = new ArrayList<>();
    SqlNode elseExpr = node.getDefaultValue().map(dv -> dv.accept(this, context)).orElse(null);

    List<WhenClause> whenClauses = node.getWhenClauses();
    for (WhenClause whenClause : whenClauses) {
      whenList.add(whenClause.getOperand().accept(this, context));
      thenList.add(whenClause.getResult().accept(this, context));
    }

    SqlCase sqlCase = new SqlCase(pos.getPos(node.getLocation()),
        null,
        new SqlNodeList(whenList, pos.getPos(node.getLocation())),
        new SqlNodeList(thenList, pos.getPos(node.getLocation())),
        elseExpr);
    return sqlCase;
  }

  @Override
  public SqlNode visitInListExpression(InListExpression node, ConvertContext context) {
    return call(node.getLocation(), SqlStdOperatorTable.IN, context,
        node.getValues().toArray(new Expression[0]));
  }

  @Override
  public SqlNode visitIdentifier(Identifier node, ConvertContext context) {
    return new SqlIdentifier(
        Arrays.stream(node.getNamePath().getNames()).map(Name::getCanonical)
            .collect(Collectors.toList()), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitIsNotNullPredicate(IsNotNullPredicate node, ConvertContext context) {
    return call(node.getLocation(), SqlStdOperatorTable.IS_NOT_NULL, context, node.getValue());
  }

  @Override
  public SqlNode visitIsNullPredicate(IsNullPredicate node, ConvertContext context) {
    return call(node.getLocation(), SqlStdOperatorTable.IS_NULL, context, node.getValue());
  }

  @Override
  public SqlNode visitNotExpression(NotExpression node, ConvertContext context) {
    return call(node.getLocation(), SqlStdOperatorTable.NOT, context, node.getValue());
  }

  @Override
  public SqlNode visitArrayConstructor(ArrayConstructor node, ConvertContext context) {
    return super.visitArrayConstructor(node, context);
  }

  @Override
  public SqlNode visitLogicalBinaryExpression(LogicalBinaryExpression node, ConvertContext context) {
    return call(node.getLocation(),
        node.getOperator() == Operator.AND ? SqlStdOperatorTable.AND : SqlStdOperatorTable.OR, context,
        node.getLeft(), node.getRight());
  }

  @Override
  public SqlNode visitSubqueryExpression(SubqueryExpression node, ConvertContext context) {
    return node.getQuery().accept(this, context);
  }

//  @Override
//  public SqlNode visitSymbolReference(SymbolReference node, ConvertContext context) {
//    return super.visitSymbolReference(node, context);
//  }

  private SqlNode call(Optional<NodeLocation> location,
      SqlOperator operator, ConvertContext context, Expression... expression) {
    return new SqlBasicCall(operator, toOperand(expression, context), pos.getPos(location));
  }

  private SqlNode[] toOperand(List<Expression> expression, ConvertContext context) {
    return toOperand(expression.toArray(Expression[]::new), context);
  }

  private SqlNode[] toOperand(Expression[] expression, ConvertContext context) {
    SqlNode[] nodes = new SqlNode[expression.length];
    for (int i = 0; i < expression.length; i++) {
      nodes[i] = expression[i].accept(this, context);
    }
    return nodes;
  }

  @Override
  public SqlNode visitLiteral(Literal node, ConvertContext context) {
    throw new RuntimeException("Unknown literal");
  }

  @Override
  public SqlNode visitDoubleLiteral(DoubleLiteral node, ConvertContext context) {
    return SqlLiteral.createExactNumeric(Double.toString(node.getValue()),
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitDecimalLiteral(DecimalLiteral node, ConvertContext context) {
    return SqlLiteral.createExactNumeric(node.getValue(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitGenericLiteral(GenericLiteral node, ConvertContext context) {
    return super.visitGenericLiteral(node, context);
  }

  @Override
  public SqlNode visitNullLiteral(NullLiteral node, ConvertContext context) {
    return SqlLiteral.createNull(pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTimeLiteral(TimeLiteral node, ConvertContext context) {
    return SqlLiteral.createTime(new TimeString(node.getValue()), 3,
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTimestampLiteral(TimestampLiteral node, ConvertContext context) {
    return SqlLiteral.createTimestamp(new TimestampString(node.getValue()), 3,
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitIntervalLiteral(IntervalLiteral node, ConvertContext context) {
    //convert to sql compliant interval
    return SqlTimeLiteral.createInterval(1, "10",
        new SqlIntervalQualifier(TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO),
        SqlParserPos.ZERO);
//    SqlNode intervalExpr = node.getExpression().accept(this, context);
//    return SqlLiteral.createInterval(node.getSign(), node.getStartField(),
//        node.get(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitStringLiteral(StringLiteral node, ConvertContext context) {
    return SqlLiteral.createCharString(node.getValue(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitBooleanLiteral(BooleanLiteral node, ConvertContext context) {
    return SqlLiteral.createBoolean(node.getValue(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitLongLiteral(LongLiteral node, ConvertContext context) {
    return SqlLiteral.createExactNumeric(Long.toString(node.getValue()),
        pos.getPos(node.getLocation()));
  }
}
