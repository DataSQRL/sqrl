package ai.dataeng.sqml.parser.sqrl;

import ai.dataeng.sqml.tree.AliasedRelation;
import ai.dataeng.sqml.tree.AllColumns;
import ai.dataeng.sqml.tree.ArithmeticBinaryExpression;
import ai.dataeng.sqml.tree.ArithmeticUnaryExpression;
import ai.dataeng.sqml.tree.ArrayConstructor;
import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.AtTimeZone;
import ai.dataeng.sqml.tree.BetweenPredicate;
import ai.dataeng.sqml.tree.BooleanLiteral;
import ai.dataeng.sqml.tree.Cast;
import ai.dataeng.sqml.tree.ComparisonExpression;
import ai.dataeng.sqml.tree.DecimalLiteral;
import ai.dataeng.sqml.tree.DistinctOn;
import ai.dataeng.sqml.tree.DoubleLiteral;
import ai.dataeng.sqml.tree.EnumLiteral;
import ai.dataeng.sqml.tree.Except;
import ai.dataeng.sqml.tree.ExistsPredicate;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.FunctionCall;
import ai.dataeng.sqml.tree.GenericLiteral;
import ai.dataeng.sqml.tree.GroupBy;
import ai.dataeng.sqml.tree.GroupingElement;
import ai.dataeng.sqml.tree.GroupingOperation;
import ai.dataeng.sqml.tree.Identifier;
import ai.dataeng.sqml.tree.InListExpression;
import ai.dataeng.sqml.tree.InPredicate;
import ai.dataeng.sqml.tree.Intersect;
import ai.dataeng.sqml.tree.IntervalLiteral;
import ai.dataeng.sqml.tree.IsNotNullPredicate;
import ai.dataeng.sqml.tree.IsNullPredicate;
import ai.dataeng.sqml.tree.Join;
import ai.dataeng.sqml.tree.JoinOn;
import ai.dataeng.sqml.tree.Limit;
import ai.dataeng.sqml.tree.Literal;
import ai.dataeng.sqml.tree.LogicalBinaryExpression;
import ai.dataeng.sqml.tree.LogicalBinaryExpression.Operator;
import ai.dataeng.sqml.tree.LongLiteral;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeLocation;
import ai.dataeng.sqml.tree.NotExpression;
import ai.dataeng.sqml.tree.NullLiteral;
import ai.dataeng.sqml.tree.OrderBy;
import ai.dataeng.sqml.tree.Query;
import ai.dataeng.sqml.tree.QueryBody;
import ai.dataeng.sqml.tree.QuerySpecification;
import ai.dataeng.sqml.tree.Relation;
import ai.dataeng.sqml.tree.Row;
import ai.dataeng.sqml.tree.Select;
import ai.dataeng.sqml.tree.SelectItem;
import ai.dataeng.sqml.tree.SetOperation;
import ai.dataeng.sqml.tree.SimpleCaseExpression;
import ai.dataeng.sqml.tree.SimpleGroupBy;
import ai.dataeng.sqml.tree.SingleColumn;
import ai.dataeng.sqml.tree.SortItem;
import ai.dataeng.sqml.tree.SortItem.Ordering;
import ai.dataeng.sqml.tree.Statement;
import ai.dataeng.sqml.tree.StringLiteral;
import ai.dataeng.sqml.tree.SubqueryExpression;
import ai.dataeng.sqml.tree.SymbolReference;
import ai.dataeng.sqml.tree.TableNode;
import ai.dataeng.sqml.tree.TableSubquery;
import ai.dataeng.sqml.tree.TimeLiteral;
import ai.dataeng.sqml.tree.TimestampLiteral;
import ai.dataeng.sqml.tree.Union;
import ai.dataeng.sqml.tree.WhenClause;
import ai.dataeng.sqml.tree.Window;
import ai.dataeng.sqml.tree.name.Name;
import com.fasterxml.jackson.databind.BeanProperty.Std;
import com.google.common.base.Preconditions;
import graphql.com.google.common.collect.ImmutableListMultimap;
import graphql.com.google.common.collect.ImmutableMap;
import graphql.com.google.common.collect.Maps;
import graphql.com.google.common.collect.Multimaps;
import io.vertx.core.MultiMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlLiteral.SqlSymbol;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
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
public class NodeToSqlNodeConverter extends AstVisitor<SqlNode, Void> {

  private final ImmutableListMultimap<String, SqlOperator> opMap;
  SqlParserPosFactory pos = new SqlParserPosFactory();

  public NodeToSqlNodeConverter() {
    this.opMap = Multimaps.index(
        SqlStdOperatorTable.instance().getOperatorList(), e->e.getName());
  }

  @Override
  public SqlNode visitNode(Node node, Void context) {
    throw new RuntimeException("Unrecognized node " + node);
  }

  @Override
  public SqlNode visitQuery(Query node, Void context) {
    //todo: order w/ unions

    return node.getQueryBody().accept(this, null);
  }

  @Override
  public SqlNode visitSelect(Select node, Void context) {
    return new SqlNodeList(
        node.getSelectItems().stream().map(s->s.accept(this, context))
                .collect(Collectors.toList()),
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitOrderBy(OrderBy node, Void context) {
    List<SqlNode> orderList = node.getSortItems().stream()
        .map(s->((SqlNode)s.accept(this, context)))
        .collect(Collectors.toList());

    return new SqlNodeList(
        orderList,
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitQuerySpecification(QuerySpecification node, Void context) {
    SqlSelect select = new SqlSelect(
        pos.getPos(node.getLocation()),
        node.getSelect().isDistinct()
            ? new SqlNodeList(
              List.of(SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, pos.getPos(node.getLocation()))),
              pos.getPos(node.getLocation()))
            : null,
        (SqlNodeList) node.getSelect().accept(this, null),
        node.getFrom().accept(this, null),
        node.getWhere().map(n->n.accept(this, null)).orElse(null),
        (SqlNodeList) node.getGroupBy().map(n->n.accept(this, null)).orElse(null),
        node.getHaving().map(n->n.accept(this, null)).orElse(null),
        null,
        (SqlNodeList) node.getOrderBy().map(n->n.accept(this, null)).orElse(null),
        null,
        node.getLimit().map(n->n.accept(this, null)).orElse(null),
        null
    );
    return select;
  }

  @Override
  public SqlNode visitSetOperation(SetOperation node, Void context) {
    return super.visitSetOperation(node, context);
  }

  @Override
  public SqlNode visitUnion(Union node, Void context) {
    return super.visitUnion(node, context);
  }

  @Override
  public SqlNode visitIntersect(Intersect node, Void context) {
    return super.visitIntersect(node, context);
  }

  @Override
  public SqlNode visitExcept(Except node, Void context) {
    return super.visitExcept(node, context);
  }

  @Override
  public SqlNode visitWhenClause(WhenClause node, Void context) {
    return super.visitWhenClause(node, context);
  }

  @Override
  public SqlNode visitSingleColumn(SingleColumn node, Void context) {
    SqlNode expr = node.getExpression().accept(this, null);
    if (node.getAlias().isPresent()) {
      return call(node.getLocation(),
          SqlStdOperatorTable.AS, node.getExpression(), node.getAlias().get());
    }
    return expr;
  }

  @Override
  public SqlNode visitAllColumns(AllColumns node, Void context) {
    throw new RuntimeException("'SELECT *' not supported in calcite node conversions");
  }

  @Override
  public SqlNode visitSortItem(SortItem node, Void context) {
    SqlNode sortKey = node.getSortKey().accept(this, null);
    if (node.getOrdering().isPresent() && node.getOrdering().get() == Ordering.DESCENDING) {
      SqlNode[] sortOps = {sortKey};
      return new SqlBasicCall(SqlStdOperatorTable.DESC, sortOps, SqlParserPos.ZERO);
    } else {
      return sortKey;
    }

//    SqlLiteral direction = SqlLiteral.createSymbol(
//        (node.getOrdering().isPresent() && node.getOrdering().get() == Ordering.ASCENDING) ?
//            Direction.ASCENDING :
//            Direction.DESCENDING
//        , pos.getPos(node.getLocation()));
//
//    return new SqlNodeList(
//        List.of(node.getSortKey().accept(this, null), direction),
//        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTable(TableNode node, Void context) {
    if (node.getAlias().isPresent()) {
      SqlIdentifier table = new SqlIdentifier(List.of(node.getNamePath().toString()), pos.getPos(node.getLocation()));
      SqlNode[] operands = {
          table,
          new SqlIdentifier(node.getAlias().get().getCanonical(), SqlParserPos.ZERO)
      };
      return new SqlBasicCall(SqlStdOperatorTable.AS, operands, pos.getPos(node.getLocation()));
    }

    return new SqlIdentifier(List.of(node.getNamePath().toString()), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTableSubquery(TableSubquery node, Void context) {
    return node.getQuery().accept(this, null);
  }

  @Override
  public SqlNode visitAliasedRelation(AliasedRelation node, Void context) {
    SqlNode[] operand = {
        node.getRelation().accept(this, null),
        node.getAlias().accept(this, null)
    };

    return new SqlBasicCall(SqlStdOperatorTable.AS, operand, pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitJoin(Join node, Void context) {
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

    SqlLiteral conditionType = node.getCriteria().map(c->c instanceof JoinOn)
        .map(on ->
            SqlLiteral.createSymbol(on ? JoinConditionType.ON : JoinConditionType.NONE, pos.getPos(node.getLocation())))
        .orElse(SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO));

    return new SqlJoin(pos.getPos(node.getLocation()),
        node.getLeft().accept(this, null),
        SqlLiteral.createBoolean(false, pos.getPos(node.getLocation())),
        SqlLiteral.createSymbol(type, pos.getPos(node.getLocation())),
        node.getRight().accept(this, null),
        conditionType,
        node.getCriteria().map(e->e.accept(this, null)).orElse(null)
    );
  }
//
//  @Override
//  public SqlNode visitWindow(Window window, Void context) {
//    return super.visitWindow(window, context);
//  }

  @Override
  public SqlNode visitJoinOn(JoinOn node, Void context) {
    return node.getExpression().accept(this, null);
  }
//
//  @Override
//  public SqlNode visitExists(ExistsPredicate node, Void context) {
//    throw new RuntimeException("ExistsPredicate Not yet implemented");
//  }
//
//  @Override
//  public SqlNode visitCast(Cast node, Void context) {
//    throw new RuntimeException("Cast Not yet implemented");
//  }
//
//  @Override
//  public SqlNode visitAtTimeZone(AtTimeZone node, Void context) {
//    throw new RuntimeException("AtTimeZone Not yet implemented");
//  }

  @Override
  public SqlNode visitGroupBy(GroupBy node, Void context) {
    return node.getGroupingElement().accept(this, null);
  }

  @Override
  public SqlNode visitSimpleGroupBy(SimpleGroupBy node, Void context) {
    return new SqlNodeList(node.getExpressions().stream().map(e->e.accept(this, null)).collect(Collectors.toList()),
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitLimitNode(Limit node, Void context) {
    return node.getIntValue().map(i->SqlLiteral.createExactNumeric(node.getValue(),
        pos.getPos(node.getLocation()))).orElse(null);
  }

  @Override
  public SqlNode visitArithmeticBinary(ArithmeticBinaryExpression node, Void context) {
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

    return call(node.getLocation(), op, node.getLeft(), node.getRight());
  }

  @Override
  public SqlNode visitBetweenPredicate(BetweenPredicate node, Void context) {
    return call(node.getLocation(), SqlStdOperatorTable.BETWEEN, node.getValue(), node.getMin(), node.getMax());
  }

  @Override
  public SqlNode visitComparisonExpression(ComparisonExpression node, Void context) {
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

    return call(node.getLocation(), op, node.getLeft(), node.getRight());
  }

  @Override
  public SqlNode visitInPredicate(InPredicate node, Void context) {
    return call(node.getLocation(), SqlStdOperatorTable.IN, node.getValueList());
  }

  @Override
  public SqlNode visitFunctionCall(FunctionCall node, Void context) {
    String opName = node.getName().get(0).getCanonical();
    List<SqlOperator> op = opMap.get(opName.toUpperCase());
    Preconditions.checkState(!op.isEmpty(), "Operation could not be found: %s", opName);

    SqlBasicCall call = new SqlBasicCall(op.get(0), toOperand(node.getArguments()), pos.getPos(node.getLocation()));

    //Convert to OVER
    if (op.get(0).requiresOver()) {
      List<SqlNode> partition = node.getOver().get().getPartitionBy().stream().map(e->e.accept(this, null)).collect(Collectors.toList());
      SqlNodeList orderList = SqlNodeList.EMPTY;
      if (node.getOver().get().getOrderBy().isPresent()) {
        OrderBy order = node.getOver().get().getOrderBy().get();
        List<SqlNode> ol = new ArrayList<>();
        for (SortItem sortItem : order.getSortItems()) {
          ol.add(sortItem.accept(this, null));
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
  public SqlNode visitSimpleCaseExpression(SimpleCaseExpression node, Void context) {
    List<SqlNode> whenList = new ArrayList<>();
    List<SqlNode> thenList = new ArrayList<>();
    SqlNode elseExpr = node.getDefaultValue().map(dv -> dv.accept(this, null)).orElse(null);

    List<WhenClause> whenClauses = node.getWhenClauses();
    for (WhenClause whenClause : whenClauses) {
      whenList.add(whenClause.getOperand().accept(this, null));
      thenList.add(whenClause.getResult().accept(this, null));
    }

    SqlCase sqlCase = new SqlCase(pos.getPos(node.getLocation()),
        null,
        new SqlNodeList(whenList, pos.getPos(node.getLocation())),
        new SqlNodeList(thenList, pos.getPos(node.getLocation())),
        elseExpr);
    return sqlCase;
  }

  @Override
  public SqlNode visitInListExpression(InListExpression node, Void context) {
    return call(node.getLocation(), SqlStdOperatorTable.IN,
        node.getValues().toArray(new Expression[0]));
  }

  @Override
  public SqlNode visitIdentifier(Identifier node, Void context) {
    return new SqlIdentifier(
        Arrays.stream(node.getNamePath().getNames()).map(Name::getCanonical)
          .collect(Collectors.toList()), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitIsNotNullPredicate(IsNotNullPredicate node, Void context) {
    return call(node.getLocation(), SqlStdOperatorTable.IS_NOT_NULL, node.getValue());
  }

  @Override
  public SqlNode visitIsNullPredicate(IsNullPredicate node, Void context) {
    return call(node.getLocation(), SqlStdOperatorTable.IS_NULL, node.getValue());
  }

  @Override
  public SqlNode visitNotExpression(NotExpression node, Void context) {
    return call(node.getLocation(), SqlStdOperatorTable.NOT, node.getValue());
  }

  @Override
  public SqlNode visitArrayConstructor(ArrayConstructor node, Void context) {
    return super.visitArrayConstructor(node, context);
  }

  @Override
  public SqlNode visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context) {
    return call(node.getLocation(),
        node.getOperator() == Operator.AND ? SqlStdOperatorTable.AND : SqlStdOperatorTable.OR,
        node.getLeft(), node.getRight());
  }

  @Override
  public SqlNode visitSubqueryExpression(SubqueryExpression node, Void context) {
    return node.getQuery().accept(this, null);
  }

//  @Override
//  public SqlNode visitSymbolReference(SymbolReference node, Void context) {
//    return super.visitSymbolReference(node, context);
//  }

  private SqlNode call(Optional<NodeLocation> location,
      SqlOperator operator, Expression... expression) {
    return new SqlBasicCall(operator, toOperand(expression), pos.getPos(location));
  }

  private SqlNode[] toOperand(List<Expression> expression) {
    return toOperand(expression.toArray(Expression[]::new));
  }

  private SqlNode[] toOperand(Expression[] expression) {
    SqlNode[] nodes = new SqlNode[expression.length];
    for (int i = 0; i < expression.length; i++) {
      nodes[i] = expression[i].accept(this, null);
    }
    return nodes;
  }

  @Override
  public SqlNode visitLiteral(Literal node, Void context) {
    throw new RuntimeException("Unknown literal");
  }

  @Override
  public SqlNode visitDoubleLiteral(DoubleLiteral node, Void context) {
    return SqlLiteral.createExactNumeric(Double.toString(node.getValue()), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitDecimalLiteral(DecimalLiteral node, Void context) {
    return SqlLiteral.createExactNumeric(node.getValue(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitGenericLiteral(GenericLiteral node, Void context) {
    return super.visitGenericLiteral(node, context);
  }

  @Override
  public SqlNode visitNullLiteral(NullLiteral node, Void context) {
    return SqlLiteral.createNull(pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTimeLiteral(TimeLiteral node, Void context) {
    return SqlLiteral.createTime(new TimeString(node.getValue()), 3, pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTimestampLiteral(TimestampLiteral node, Void context) {
    return SqlLiteral.createTimestamp(new TimestampString(node.getValue()), 3, pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitIntervalLiteral(IntervalLiteral node, Void context) {
    //convert to sql compliant interval
    return SqlTimeLiteral.createInterval(1, "10",
        new SqlIntervalQualifier(TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO), SqlParserPos.ZERO);
//    SqlNode intervalExpr = node.getExpression().accept(this, null);
//    return SqlLiteral.createInterval(node.getSign(), node.getStartField(),
//        node.get(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitStringLiteral(StringLiteral node, Void context) {
    return SqlLiteral.createCharString(node.getValue(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitBooleanLiteral(BooleanLiteral node, Void context) {
    return SqlLiteral.createBoolean(node.getValue(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitLongLiteral(LongLiteral node, Void context) {
    return SqlLiteral.createExactNumeric(Long.toString(node.getValue()), pos.getPos(node.getLocation()));
  }
}
