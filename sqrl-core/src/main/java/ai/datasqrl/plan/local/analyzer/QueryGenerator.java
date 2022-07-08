package ai.datasqrl.plan.local.analyzer;

import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AS;

import ai.datasqrl.parse.tree.AliasedRelation;
import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.ArithmeticBinaryExpression;
import ai.datasqrl.parse.tree.ArrayConstructor;
import ai.datasqrl.parse.tree.BetweenPredicate;
import ai.datasqrl.parse.tree.BooleanLiteral;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.DecimalLiteral;
import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
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
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.local.analyzer.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.analyzer.QueryGenerator.Scope;
import com.google.common.base.Preconditions;
import graphql.com.google.common.collect.ImmutableListMultimap;
import graphql.com.google.common.collect.Multimaps;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.Value;
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
import org.apache.calcite.sql.SqlTimeLiteral;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

/**
 * Generates a query based on a script, the analysis, and the calcite schema.
 */
@Getter
public class QueryGenerator extends DefaultTraversalVisitor<SqlNode, Scope> {

  protected final Analysis analysis;
  private final ImmutableListMultimap<String, SqlOperator> opMap;
  protected Map<Name, AbstractSqrlTable> tables = new HashMap<>();
  SqlParserPosFactory pos = new SqlParserPosFactory();

  public QueryGenerator(Analysis analysis) {
    this.analysis = analysis;
    //todo: move
    this.opMap = Multimaps.index(SqrlOperatorTable.instance().getOperatorList(), e -> e.getName());
  }

  @Override
  public SqlNode visitNode(Node node, Scope context) {
    throw new RuntimeException(
        "Unrecognized node " + node + ":" + ((node != null) ? node.getClass().getName() : ""));
  }

  @Override
  public SqlNode visitQuery(Query node, Scope context) {
    return node.getQueryBody().accept(this, context);
  }

  @Override
  public SqlNode visitQuerySpecification(QuerySpecification node, Scope context) {
    SqlSelect select = new SqlSelect(pos.getPos(node.getLocation()),
        node.getSelect().isDistinct() ? new SqlNodeList(List.of(
            SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, pos.getPos(node.getLocation()))),
            pos.getPos(node.getLocation())) : null,
        (SqlNodeList) node.getSelect().accept(this, context), node.getFrom().accept(this, context),
        node.getWhere().map(n -> n.accept(this, context)).orElse(null),
        (SqlNodeList) node.getGroupBy().map(n -> n.accept(this, context)).orElse(null),
        node.getHaving().map(n -> n.accept(this, context)).orElse(null), null,
        (SqlNodeList) node.getOrderBy().map(n -> n.accept(this, context)).orElse(null), null,
        node.getLimit().map(n -> n.accept(this, context)).orElse(null), null);

    //If nested & limit, transform to rownum
    //Add parent primary keys


    return select;
  }

  @Override
  public SqlNode visitSelect(Select node, Scope context) {
    return new SqlNodeList(node.getSelectItems().stream().map(s -> s.accept(this, context))
        .collect(Collectors.toList()), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitOrderBy(OrderBy node, Scope context) {
    List<SqlNode> orderList = node.getSortItems().stream().map(s -> s.accept(this, context))
        .collect(Collectors.toList());

    return new SqlNodeList(orderList, pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitUnion(Union node, Scope context) {
    List<SqlNode> queries = node.getRelations().stream().map(x -> x.accept(this, context))
        .collect(Collectors.toList());
    SqlSetOperator op =
        node.isDistinct().orElse(false) ? SqlStdOperatorTable.UNION_ALL : SqlStdOperatorTable.UNION;
    return new SqlBasicCall(op, queries.toArray(new SqlNode[0]), SqlParserPosFactory.from(node));
  }


  @Override
  public SqlNode visitIntersect(Intersect node, Scope context) {
    List<SqlNode> queries = node.getRelations().stream().map(x -> x.accept(this, context))
        .collect(Collectors.toList());
    SqlSetOperator op = node.isDistinct().orElse(false) ? SqlStdOperatorTable.INTERSECT_ALL
        : SqlStdOperatorTable.INTERSECT;
    return new SqlBasicCall(op, queries.toArray(new SqlNode[0]), SqlParserPosFactory.from(node));
  }

  @Override
  public SqlNode visitExcept(Except node, Scope context) {
    List<SqlNode> queries = node.getRelations().stream().map(x -> x.accept(this, context))
        .collect(Collectors.toList());
    SqlSetOperator op = node.isDistinct().orElse(false) ? SqlStdOperatorTable.EXCEPT_ALL
        : SqlStdOperatorTable.EXCEPT;
    return new SqlBasicCall(op, queries.toArray(new SqlNode[0]), SqlParserPosFactory.from(node));
  }

  @Override
  public SqlNode visitSingleColumn(SingleColumn node, Scope context) {
    SqlNode expr = node.getExpression().accept(this, context);
    if (node.getAlias().isPresent()) {
      return call(node.getLocation(), AS, context, node.getExpression(), node.getAlias().get());
    }
    return expr;
  }

  @Override
  public SqlNode visitAllColumns(AllColumns node, Scope context) {
    return new SqlIdentifier("*", SqlParserPosFactory.from(node));
  }

  @Override
  public SqlNode visitSortItem(SortItem node, Scope context) {
    SqlNode sortKey = node.getSortKey().accept(this, context);
    if (node.getOrdering().isPresent() && node.getOrdering().get() == Ordering.DESCENDING) {
      SqlNode[] sortOps = {sortKey};
      return new SqlBasicCall(SqlStdOperatorTable.DESC, sortOps, SqlParserPos.ZERO);
    }

    return sortKey;
  }

  @Override
  public SqlNode visitTableNode(TableNode node, Scope context) {
    ResolvedNamePath resolvedTable = analysis.getResolvedNamePath().get(node);
    String name = resolvedTable.getToTable().getId().getCanonical();
    if (node.getAlias().isPresent()) {
      SqlIdentifier table = new SqlIdentifier(List.of(name), pos.getPos(node.getLocation()));
      SqlNode[] operands = {table,
          new SqlIdentifier(node.getAlias().get().getCanonical(), SqlParserPos.ZERO)};
      return new SqlBasicCall(AS, operands, pos.getPos(node.getLocation()));
    }

    return new SqlIdentifier(List.of(name), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTableSubquery(TableSubquery node, Scope context) {
    return node.getQuery().accept(this, context);
  }

  @Override
  public SqlNode visitAliasedRelation(AliasedRelation node, Scope context) {
    SqlNode[] operand = {node.getRelation().accept(this, context),
        node.getAlias().accept(this, context)};

    return new SqlBasicCall(AS, operand, pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitJoin(Join node, Scope context) {
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

    SqlLiteral conditionType = node.getCriteria().map(c -> c instanceof JoinOn).map(
            on -> SqlLiteral.createSymbol(on ? JoinConditionType.ON : JoinConditionType.NONE,
                pos.getPos(node.getLocation())))
        .orElse(SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO));

    return new SqlJoin(pos.getPos(node.getLocation()), node.getLeft().accept(this, context),
        SqlLiteral.createBoolean(false, pos.getPos(node.getLocation())),
        SqlLiteral.createSymbol(type, pos.getPos(node.getLocation())),
        node.getRight().accept(this, context), conditionType,
        node.getCriteria().map(e -> e.accept(this, context)).orElse(null));
  }
//
//  @Override
//  public SqlNode visitWindow(Window window, Scope context) {
//    return super.visitWindow(window, context);
//  }

  @Override
  public SqlNode visitJoinOn(JoinOn node, Scope context) {
    return node.getExpression().accept(this, context);
  }
//
//  @Override
//  public SqlNode visitExists(ExistsPredicate node, Scope context) {
//    throw new RuntimeException("ExistsPredicate Not yet implemented");
//  }
//
//  @Override
//  public SqlNode visitCast(Cast node, Scope context) {
//    throw new RuntimeException("Cast Not yet implemented");
//  }
//
//  @Override
//  public SqlNode visitAtTimeZone(AtTimeZone node, Scope context) {
//    throw new RuntimeException("AtTimeZone Not yet implemented");
//  }

  @Override
  public SqlNode visitGroupBy(GroupBy node, Scope context) {
    return node.getGroupingElement().accept(this, context);
  }

  @Override
  public SqlNode visitSimpleGroupBy(SimpleGroupBy node, Scope context) {
    return new SqlNodeList(node.getExpressions().stream().map(e -> e.accept(this, context))
        .collect(Collectors.toList()), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitLimitNode(Limit node, Scope context) {
    return node.getIntValue()
        .map(i -> SqlLiteral.createExactNumeric(node.getValue(), pos.getPos(node.getLocation())))
        .orElse(null);
  }

  @Override
  public SqlNode visitArithmeticBinary(ArithmeticBinaryExpression node, Scope context) {
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
  public SqlNode visitBetweenPredicate(BetweenPredicate node, Scope context) {
    return call(node.getLocation(), SqlStdOperatorTable.BETWEEN, context, node.getValue(),
        node.getMin(), node.getMax());
  }

  @Override
  public SqlNode visitComparisonExpression(ComparisonExpression node, Scope context) {
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
  public SqlNode visitInPredicate(InPredicate node, Scope context) {
    return call(node.getLocation(), SqlStdOperatorTable.IN, context, node.getValueList());
  }

  @Override
  public SqlNode visitFunctionCall(FunctionCall node, Scope context) {
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

      SqlNode[] operands = {call,
          new SqlWindow(pos.getPos(node.getLocation()), null, null, partitionList, orderList,
              SqlLiteral.createBoolean(false, pos.getPos(node.getLocation())), null, null, null)};

      return new SqlBasicCall(SqlStdOperatorTable.OVER, operands, pos.getPos(node.getLocation()));
    }

    return call;
  }

  @Override
  public SqlNode visitSimpleCaseExpression(SimpleCaseExpression node, Scope context) {
    List<SqlNode> whenList = new ArrayList<>();
    List<SqlNode> thenList = new ArrayList<>();
    SqlNode elseExpr = node.getDefaultValue().map(dv -> dv.accept(this, context)).orElse(null);

    List<WhenClause> whenClauses = node.getWhenClauses();
    for (WhenClause whenClause : whenClauses) {
      whenList.add(whenClause.getOperand().accept(this, context));
      thenList.add(whenClause.getResult().accept(this, context));
    }

    SqlCase sqlCase = new SqlCase(pos.getPos(node.getLocation()), null,
        new SqlNodeList(whenList, pos.getPos(node.getLocation())),
        new SqlNodeList(thenList, pos.getPos(node.getLocation())), elseExpr);
    return sqlCase;
  }

  @Override
  public SqlNode visitInListExpression(InListExpression node, Scope context) {
    return call(node.getLocation(), SqlStdOperatorTable.IN, context,
        node.getValues().toArray(new Expression[0]));
  }

  @Override
  public SqlNode visitIdentifier(Identifier node, Scope context) {
    return new SqlIdentifier(Arrays.stream(node.getNamePath().getNames()).map(Name::getCanonical)
        .collect(Collectors.toList()), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitIsNotNullPredicate(IsNotNullPredicate node, Scope context) {
    return call(node.getLocation(), SqlStdOperatorTable.IS_NOT_NULL, context, node.getValue());
  }

  @Override
  public SqlNode visitIsNullPredicate(IsNullPredicate node, Scope context) {
    return call(node.getLocation(), SqlStdOperatorTable.IS_NULL, context, node.getValue());
  }

  @Override
  public SqlNode visitNotExpression(NotExpression node, Scope context) {
    return call(node.getLocation(), SqlStdOperatorTable.NOT, context, node.getValue());
  }

  @Override
  public SqlNode visitArrayConstructor(ArrayConstructor node, Scope context) {
    return super.visitArrayConstructor(node, context);
  }

  @Override
  public SqlNode visitLogicalBinaryExpression(LogicalBinaryExpression node, Scope context) {
    return call(node.getLocation(),
        node.getOperator() == Operator.AND ? SqlStdOperatorTable.AND : SqlStdOperatorTable.OR,
        context, node.getLeft(), node.getRight());
  }

  @Override
  public SqlNode visitSubqueryExpression(SubqueryExpression node, Scope context) {
    return node.getQuery().accept(this, context);
  }

//  @Override
//  public SqlNode visitSymbolReference(SymbolReference node, Scope context) {
//    return super.visitSymbolReference(node, context);
//  }

  private SqlNode call(Optional<NodeLocation> location, SqlOperator operator, Scope context,
      Expression... expression) {
    return new SqlBasicCall(operator, toOperand(expression, context), pos.getPos(location));
  }

  private SqlNode[] toOperand(List<Expression> expression, Scope context) {
    return toOperand(expression.toArray(Expression[]::new), context);
  }

  private SqlNode[] toOperand(Expression[] expression, Scope context) {
    SqlNode[] nodes = new SqlNode[expression.length];
    for (int i = 0; i < expression.length; i++) {
      nodes[i] = expression[i].accept(this, context);
    }
    return nodes;
  }

  @Override
  public SqlNode visitLiteral(Literal node, Scope context) {
    throw new RuntimeException("Unknown literal");
  }

  @Override
  public SqlNode visitDoubleLiteral(DoubleLiteral node, Scope context) {
    return SqlLiteral.createExactNumeric(Double.toString(node.getValue()),
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitDecimalLiteral(DecimalLiteral node, Scope context) {
    return SqlLiteral.createExactNumeric(node.getValue(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitGenericLiteral(GenericLiteral node, Scope context) {
    return super.visitGenericLiteral(node, context);
  }

  @Override
  public SqlNode visitNullLiteral(NullLiteral node, Scope context) {
    return SqlLiteral.createNull(pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTimeLiteral(TimeLiteral node, Scope context) {
    return SqlLiteral.createTime(new TimeString(node.getValue()), 3,
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitTimestampLiteral(TimestampLiteral node, Scope context) {
    return SqlLiteral.createTimestamp(new TimestampString(node.getValue()), 3,
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitIntervalLiteral(IntervalLiteral node, Scope context) {
    //convert to sql compliant interval
    return SqlTimeLiteral.createInterval(1, "10",
        new SqlIntervalQualifier(TimeUnit.SECOND, TimeUnit.SECOND, SqlParserPos.ZERO),
        SqlParserPos.ZERO);
//    SqlNode intervalExpr = node.getExpression().accept(this, context);
//    return SqlLiteral.createInterval(node.getSign(), node.getStartField(),
//        node.get(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitStringLiteral(StringLiteral node, Scope context) {
    return SqlLiteral.createCharString(node.getValue(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitBooleanLiteral(BooleanLiteral node, Scope context) {
    return SqlLiteral.createBoolean(node.getValue(), pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitLongLiteral(LongLiteral node, Scope context) {
    return SqlLiteral.createExactNumeric(Long.toString(node.getValue()),
        pos.getPos(node.getLocation()));
  }

  private SqlNodeList convertHints(List<Hint> hints) {
    if (hints.isEmpty()) {
      return SqlNodeList.EMPTY;
    }
    return new SqlNodeList(hints.stream().map(h -> convertHint(h)).collect(Collectors.toList()),
        pos.getPos(Optional.empty()));
  }

  private SqlHint convertHint(Hint h) {
    return new SqlHint(pos.getPos(Optional.empty()),
        new SqlIdentifier(h.getValue(), pos.getPos(Optional.empty())), SqlNodeList.EMPTY,
        HintOptionFormat.EMPTY);
  }


  @Value
  public static class Scope {
    Analysis analysis;
    QueryGenerator calcite;
  }

  /**
   * Import:
   *  - Resolve abstract tables
   *
   * Expression:
   *  - Convert to sql query via sql generator, plan, add to table map
   *
   * Query:
   *  - Convert to sql query via sql generator, plan, add to table map
   *
   * Join declaration:
   *  - ...
   *
   * Calcite schema then has a full DAG and local execution plan
   *
   * We then pass it to the global optimizer
   *
   */

  /**
   * Brainstorm:
   *
   * Primary keys:
   * - Needed for join conditions
   * - Needed to determine cardinality (!)
   * - Needed for windows
   *
   * Join expansion:
   * - Need to store the join declaration somewhere
   * - Relationships to paths.
   *
   * One schema pattern:
   * - TableVersion has calcite information ?
   *
   * When do we give it to the implementer?
   * Go over the entire annotated script:
   *
   *
   * When do we introduce _uuid + index etc:
   *
   *
   *First the customer writes up a script for how he wants us to implement it. He mails it to us
   * for processing.
   *
   * At the office, the analyzer gets the script and goes over it to understand it. He annotates
   * all of the symbols for what they should mean. He will need to also derive primary keys for
   * distinct & group by statements, otherwise the inline paths are assumed to be to-many unless
   * there are explicit LIMIT 1s. All tables have a _id primary key which could be derived from
   * other fields later. The analyzer can deduce the cardinality of all pathed relationships.
   *
   * Next the analyzer passes this information to the generator.
   *
   * The generator's responsibility is to convert the annotated script into our internal format
   * to be processed. The generator uses a set of tools to reduce the difficulty of their job.
   * The generator will create a schema with all of the extra fields to complete the queries.
   * They will create relationships between these tables using this schema. The generator will
   * create a schema and all of the sql queries they need to populate the tables.
   *
   * The calcite schema will now have all of the logical plans and how they are connected. We
   * then go through the list of queries the user wants answered and generate logical plans for
   * each one. The logical plans are then passed to the optimizer.
   *
   * The optimizer's responsibility is to take the logical plans and find efficient ways of
   * executing them. The optimizer will use a cost model to determine the best way to execute the
   * query. The optimizer will also resolve any ambiguities in the logical plans. The optimizer
   * will also check to see if any of the logical plans can be executed in parallel.
   *
   * The optimizer will then pass the optimized logical plans to the physical planner.
   *
   * The physical planner's responsibility is to take the optimized logical plans and find the
   * best way to execute them on the hardware. The physical planner will use a cost model to
   * determine the best way to execute the query. The physical planner will also resolve any
   * ambiguities in the logical plans. The physical planner will also check to see if any of the
   * logical plans can be executed in parallel.
   *
   * The physical planner will then pass the physical plans to the executor.
   *
   * The executor's responsibility is to take the physical plans and execute them. The executor
   * will use a cost model to determine the best way to execute the query. The executor will also
   * resolve any ambiguities in the physical plans. The executor will also check to see if any of
   * the physical plans can be executed in parallel.
   *
   * The executor will then return the results of the query to the user.
   *
   */
}
