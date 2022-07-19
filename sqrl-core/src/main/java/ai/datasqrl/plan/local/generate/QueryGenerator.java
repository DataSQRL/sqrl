package ai.datasqrl.plan.local.generate;

import static ai.datasqrl.plan.local.generate.node.util.SqlNodeUtil.and;

import ai.datasqrl.parse.tree.AllColumns;
import ai.datasqrl.parse.tree.ArithmeticBinaryExpression;
import ai.datasqrl.parse.tree.ArrayConstructor;
import ai.datasqrl.parse.tree.BetweenPredicate;
import ai.datasqrl.parse.tree.BooleanLiteral;
import ai.datasqrl.parse.tree.ComparisonExpression;
import ai.datasqrl.parse.tree.DecimalLiteral;
import ai.datasqrl.parse.tree.DefaultTraversalVisitor;
import ai.datasqrl.parse.tree.DistinctAssignment;
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
import ai.datasqrl.parse.tree.Join.Type;
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
import ai.datasqrl.parse.tree.Relation;
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
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.local.analyze.Analysis;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedFunctionCall;
import ai.datasqrl.plan.local.analyze.Analysis.ResolvedNamePath;
import ai.datasqrl.plan.local.generate.QueryGenerator.Scope;
import ai.datasqrl.plan.local.generate.node.SqlJoinDeclaration;
import ai.datasqrl.plan.local.generate.node.SqlResolvedIdentifier;
import ai.datasqrl.plan.local.generate.node.builder.JoinPathBuilder;
import ai.datasqrl.plan.local.generate.node.builder.LocalAggBuilder;
import ai.datasqrl.plan.local.generate.node.util.SqlNodeUtil;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Table;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Getter;
import lombok.Setter;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlHint.HintOptionFormat;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOrderBy;
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
import org.codehaus.commons.nullanalysis.Nullable;

/**
 * Generates a query based on a script, the analysis, and the calcite schema.
 */
@Getter
public class QueryGenerator extends DefaultTraversalVisitor<SqlNode, Scope> {

  protected final Analysis analysis;
  private final Map<Relationship, SqlJoinDeclaration> joins = new HashMap<>();
  protected Map<String, AbstractSqrlTable> tables = new HashMap<>();
  protected Map<Field, String> fieldNames = new HashMap<>();
  protected Map<Table, AbstractSqrlTable> tableMap = new HashMap<>();

  SqlParserPosFactory pos = new SqlParserPosFactory();

  public QueryGenerator(Analysis analysis) {
    this.analysis = analysis;
  }

  private List<SqlNode> visit(List<? extends Node> nodes, Scope context) {
    return nodes.stream().map(n -> n.accept(this, context)).collect(Collectors.toList());
  }

  @Override
  public SqlNode visitNode(Node node, Scope context) {
    throw new RuntimeException(
        "Unrecognized node " + node + ":" + ((node != null) ? node.getClass().getName() : ""));
  }

  @Override
  public SqlNode visitQuery(Query node, Scope context) {
    if (node.getQueryBody() instanceof SetOperation && (node.getOrderBy().isPresent()
        || node.getLimit().isPresent())) {
      //Can be empty if limit exists
      List<SqlNode> orderList = node.getOrderBy().map(o -> visit(o.getSortItems(), context))
          .orElse(List.of());

      Optional<SqlNode> limit = node.getLimit().map(l -> l.accept(this, context));
      SqlNode query = node.getQueryBody().accept(this, context);
      SqlOrderBy order = new SqlOrderBy(SqlParserPos.ZERO, query,
          new SqlNodeList(orderList, SqlParserPos.ZERO), null, limit.orElse(null));

      return order;
    }

    return node.getQueryBody().accept(this, context);
  }

  @Override
  public SqlNode visitQuerySpecification(QuerySpecification node, Scope context) {
    List<SqlNode> ppk = parentPrimaryKeys(context);
    List<SqlNode> ppkIndex = IntStream.range(0, ppk.size())
        .mapToObj(i -> SqlLiteral.createExactNumeric(Long.toString(i + 1), SqlParserPos.ZERO))
        .collect(Collectors.toList());
    SqlNodeList keywords = keywords(node.getSelect());
    SqlNodeList select = (SqlNodeList) node.getSelect().accept(this, context);
    context.setPPKOffset(ppk.size());
    select = prepend(select, ppk);
    context.setOffset(ppk.size());
    SqlNodeList groupBy = (SqlNodeList) node.getGroupBy().map(n -> n.accept(this, context))
        .orElse(null);
    groupBy = prepend(groupBy, ppkIndex);
    SqlNode having = node.getHaving().map(n -> n.accept(this, context)).orElse(null);
    SqlNodeList orderBy = (SqlNodeList) node.getOrderBy().map(n -> n.accept(this, context))
        .orElse(null);
    orderBy = prepend(orderBy, ppkIndex);
    SqlNode where = node.getWhere().map(n -> n.accept(this, context)).orElse(null);
    SqlNode limit = node.getLimit().map(n -> n.accept(this, context)).orElse(null);

    //Note: Group by still required for distinct:
    //e.g. SELECT DISTINCT COUNT(*) FROM Product GROUP BY name;
    if (isNestedLimitDistinct(node, context)) {
      keywords = null;
      select = append(select, rownumber(), rank(), denserank());
//      where = and(where, filterNestedLimitDistinct(node));
      limit = null;
    } else if (isNestedDistinct(node, context)) {
      keywords = null;
      select = append(select/*, rownumber(), rank()*/);
//      where = and(where, filterNestedDistinct(node));
    } else if (isNestedLimit(node, context)) {
      select = append(select, rownumber());
//      where = and(where, filterLimit(node));
      limit = null;
    }

    Relation relation = node.getFrom();
    if (analysis.getNeedsSelfTableJoin().contains(node)) {
      //Transform using sqrl ast
      TableNode n = analysis.getSelfTableNode().get(node);

      relation = new Join(Type.CROSS, n,
          relation,
          Optional.empty());
    }
    //If doesn't have _ and is nested, need to add it.
    SqlNode from = relation.accept(this, context);

//    if (!context.getSubqueries().isEmpty()) {
    for (int i = 0; i < context.getAddlJoins().size(); i++) {
      SqlJoinDeclaration join = context.getAddlJoins().get(i);
      SqlLiteral conditionType;
      if (join.getTrailingCondition().isPresent()) {
        conditionType = SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO);
      } else {
        conditionType = SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO);
      }
      SqlLiteral joinType = SqlLiteral.createSymbol(org.apache.calcite.sql.JoinType.LEFT,
          SqlParserPos.ZERO);
      from = new SqlJoin(SqlParserPos.ZERO, from,
          SqlLiteral.createBoolean(false, SqlParserPos.ZERO), joinType, join.getRel(),
          conditionType, join.getTrailingCondition().orElse(null));
    }
//    }

    SqlSelect query = new SqlSelect(pos.getPos(node.getLocation()), keywords, select, from, where,
        groupBy.getList().isEmpty()? null: groupBy, having, null, orderBy, null, limit, null);

    //Add parent primary keys

    return query;
  }

  private SqlNode filterLimit(QuerySpecification node) {
    if (node.getLimit().get().getIntValue().isEmpty()) {
      return null;
    }
    Integer limit = node.getLimit().get().getIntValue().get();

    SqlBinaryOperator op =
        (limit.equals(1)) ? SqrlOperatorTable.EQUALS : SqrlOperatorTable.LESS_THAN_OR_EQUAL;

    return new SqlBasicCall(op, new SqlNode[]{new SqlIdentifier("__row_number", SqlParserPos.ZERO),
        SqlLiteral.createApproxNumeric(node.getLimit().map(l -> l.getValue()).orElseThrow(),
            SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }

  private SqlNode filterNestedDistinct(QuerySpecification node) {
    return new SqlBasicCall(SqrlOperatorTable.EQUALS,
        new SqlNode[]{new SqlIdentifier("__row_number", SqlParserPos.ZERO),
            new SqlIdentifier("__rank", SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }

  private SqlNode filterNestedLimitDistinct(QuerySpecification node) {
    return and(new SqlBasicCall(SqrlOperatorTable.EQUALS,
            new SqlNode[]{new SqlIdentifier("__row_number", SqlParserPos.ZERO),
                new SqlIdentifier("__rank", SqlParserPos.ZERO)}, SqlParserPos.ZERO),
        new SqlBasicCall(SqrlOperatorTable.EQUALS,
            new SqlNode[]{new SqlIdentifier("__denserank", SqlParserPos.ZERO),
                SqlLiteral.createApproxNumeric(node.getLimit().map(Limit::getValue).orElseThrow(),
                    SqlParserPos.ZERO)}, SqlParserPos.ZERO));
  }

  private SqlNode rank() {
    SqlWindow window = window(SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    return as(
        over(new SqlBasicCall(SqrlOperatorTable.RANK, new SqlNode[0], SqlParserPos.ZERO), window),
        "__rank");
  }

  private SqlNode as(SqlNode node, String name) {
    return new SqlBasicCall(SqrlOperatorTable.AS,
        new SqlNode[]{node, new SqlIdentifier(name, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }

  private SqlWindow window(SqlNodeList partition, SqlNodeList order) {
    return new SqlWindow(SqlParserPos.ZERO, null, null, partition, order,
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO), null, null, null);
  }

  private SqlNode over(SqlBasicCall call, SqlWindow window) {
    return new SqlBasicCall(SqrlOperatorTable.OVER, new SqlNode[]{call, window}, SqlParserPos.ZERO);
  }

  private SqlNode denserank() {
    SqlWindow window = window(SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    return as(
        over(new SqlBasicCall(SqrlOperatorTable.DENSE_RANK, new SqlNode[0], SqlParserPos.ZERO),
            window), "__denserank");
  }

  private SqlNode rownumber() {
    SqlWindow window = window(SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    return as(
        over(new SqlBasicCall(SqrlOperatorTable.ROW_NUMBER, new SqlNode[0], SqlParserPos.ZERO),
            window), "__row_number");
  }

  private boolean isNestedLimitDistinct(QuerySpecification node, Scope context) {
    return context.isNested() && node.getLimit().isPresent() && node.getSelect().isDistinct();
  }

  private boolean isNestedDistinct(QuerySpecification node, Scope context) {
    return context.isNested() && node.getSelect().isDistinct();
  }

  private boolean isNestedLimit(QuerySpecification node, Scope context) {
    return context.isNested() && node.getLimit().isPresent();
  }

  private List<SqlNode> parentPrimaryKeys(Scope context) {
    if (context.getParentTable().isEmpty()) {
      return List.of();
    }

    return context.getParentTable().get().getPrimaryKeys().stream()
        .map(e -> new SqlIdentifier(List.of("_", e), SqlParserPos.ZERO))
        .collect(Collectors.toList());
  }

  private SqlNodeList prepend(SqlNodeList nodeList, List<SqlNode> toAdd) {
    return prepend(nodeList, toAdd.toArray(new SqlNode[0]));
  }

  private SqlNodeList prepend(@Nullable SqlNodeList nodeList, SqlNode... toAdd) {
    if (nodeList == null) {
      return new SqlNodeList(List.of(toAdd), SqlParserPos.ZERO);
    }
    List<SqlNode> list = new ArrayList<>(List.of(toAdd));
    list.addAll(nodeList.getList());
    return new SqlNodeList(list, nodeList.getParserPosition());
  }

  private SqlNodeList append(SqlNodeList nodeList, List<SqlNode> toAdd) {
    return append(nodeList, toAdd.toArray(new SqlNode[0]));
  }

  private SqlNodeList append(@Nullable SqlNodeList nodeList, SqlNode... toAdd) {
    if (nodeList == null) {
      return new SqlNodeList(List.of(toAdd), nodeList.getParserPosition());
    }
    List<SqlNode> list = new ArrayList<>(nodeList.getList());
    list.addAll(List.of(toAdd));
    return new SqlNodeList(list, nodeList.getParserPosition());
  }

  private SqlNodeList keywords(Select select) {
    return select.isDistinct() ? new SqlNodeList(List.of(
        SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, SqlParserPosFactory.getPos(select))),
        SqlParserPosFactory.getPos(select)) : null;
  }

  @Override
  public SqlNode visitSelect(Select node, Scope context) {
    /**
     * if agg, add ppk
     */

    List<SingleColumn> columns = analysis.getSelectItems().get(node);
    return new SqlNodeList(
        columns.stream().map(s -> s.accept(this, context)).collect(Collectors.toList()),
        pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitOrderBy(OrderBy node, Scope context) {
    List<SqlNode> orderList = analysis.getOrderByExpressions().stream().map(
            s -> s.getSortKey() instanceof LongLiteral ? new SortItem(s.getLocation(),
                offset((LongLiteral) s.getSortKey(), context), s.getOrdering()) : s)
        .map(s -> s.accept(this, context)).collect(Collectors.toList());

    return new SqlNodeList(orderList, pos.getPos(node.getLocation()));
  }

  private Expression offset(LongLiteral sortKey, Scope context) {
    return new LongLiteral(sortKey.getLocation(),
        Long.toString(sortKey.getValue() + context.getOffset()));
  }

  @Override
  public SqlNode visitUnion(Union node, Scope context) {
    List<SqlNode> queries = node.getRelations().stream().map(x -> x.accept(this, context))
        .collect(Collectors.toList());
    //TODO: line up columns by name.
    SqlSetOperator op =
        node.isDistinct().orElse(false) ? SqlStdOperatorTable.UNION_ALL : SqlStdOperatorTable.UNION;
    return new SqlBasicCall(op, queries.toArray(new SqlNode[0]), SqlParserPosFactory.getPos(node));
  }

  @Override
  public SqlNode visitIntersect(Intersect node, Scope context) {
    List<SqlNode> queries = node.getRelations().stream().map(x -> x.accept(this, context))
        .collect(Collectors.toList());
    SqlSetOperator op = node.isDistinct().orElse(false) ? SqlStdOperatorTable.INTERSECT_ALL
        : SqlStdOperatorTable.INTERSECT;
    return new SqlBasicCall(op, queries.toArray(new SqlNode[0]), SqlParserPosFactory.getPos(node));
  }

  @Override
  public SqlNode visitExcept(Except node, Scope context) {
    List<SqlNode> queries = node.getRelations().stream().map(x -> x.accept(this, context))
        .collect(Collectors.toList());
    SqlSetOperator op = node.isDistinct().orElse(false) ? SqlStdOperatorTable.EXCEPT_ALL
        : SqlStdOperatorTable.EXCEPT;
    return new SqlBasicCall(op, queries.toArray(new SqlNode[0]), SqlParserPosFactory.getPos(node));
  }

  @Override
  public SqlNode visitSingleColumn(SingleColumn node, Scope context) {
    SqlNode expr = node.getExpression().accept(this, context);
    if (node.getAlias().isPresent()) {
      return new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{expr,
          new SqlIdentifier(node.getAlias().get().getNamePath().getFirst().getCanonical(),
              SqlParserPos.ZERO)}, SqlParserPos.ZERO);
    }
    return expr;
  }

  @Override
  public SqlNode visitAllColumns(AllColumns node, Scope context) {
    throw new RuntimeException("");
  }

  @Override
  public SqlNode visitSortItem(SortItem node, Scope context) {
    SqlNode sortKey = node.getSortKey().accept(this, context);
    if (node.getOrdering().isPresent() && node.getOrdering().get() == Ordering.DESCENDING) {
      return new SqlBasicCall(SqlStdOperatorTable.DESC, new SqlNode[]{sortKey}, SqlParserPos.ZERO);
    }

    return sortKey;
  }

  /**
   * FROM _ JOIN _.entries;
   * <p>
   * FROM Order JOIN entries on Order.pk = e.pk
   */
  @Override
  public SqlNode visitTableNode(TableNode node, Scope context) {
    ResolvedNamePath resolvedTable = analysis.getResolvedNamePath().get(node);
    JoinPathBuilder builder = new JoinPathBuilder(joins, tableMap);
    SqlJoinDeclaration join = builder.expand(resolvedTable, node.getAlias());
    if (join.getTrailingCondition().isPresent()) {
      context.setPullupCondition(join.getTrailingCondition().get());
    }

    return join.getRel();
  }

  @Override
  public SqlNode visitTableSubquery(TableSubquery node, Scope context) {
    throw new RuntimeException("TBD");
//    return node.getQuery().accept(this, context);
  }

  @Override
  public SqlNode visitJoin(Join node, Scope context) {
    JoinType type;
    switch (node.getType()) {
      case INNER:
        type = JoinType.INNER;
        break;
      case DEFAULT:
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

    SqlParserPos ppos = pos.getPos(node.getLocation());
    SqlLiteral natural = SqlLiteral.createBoolean(false, pos.getPos(node.getLocation()));
    SqlNode lhs = node.getLeft().accept(this, context);
    SqlNode rhs = node.getRight().accept(this, context);

    SqlNode criteria = node.getCriteria().map(e -> e.accept(this, context)).orElse(null);
    Optional<SqlNode> addlCondition = context.consumePullupCondition();
    SqlNode addedCriteria = addlCondition.map(c ->
        and(criteria, c)).orElse(criteria);
    if (addlCondition.isPresent()) {
      type = JoinType.LEFT;
    }
    SqlLiteral joinType = SqlLiteral.createSymbol(type, pos.getPos(node.getLocation()));


    SqlLiteral conditionType;
    if (addlCondition.isPresent()) {
      conditionType = SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO);
    } else {
      conditionType = SqlLiteral.createSymbol(JoinConditionType.NONE, SqlParserPos.ZERO);
    }

    return new SqlJoin(ppos, lhs, natural, joinType, rhs, conditionType, addedCriteria);
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
    //is aggregating, add group by

    List<SqlNode> ordinals = analysis.groupByOrdinals.stream().map(
        i -> SqlLiteral.createExactNumeric(Integer.toString(i + 1 + context.offset),
            SqlParserPos.ZERO)).collect(Collectors.toList());

    return new SqlNodeList(ordinals, pos.getPos(node.getLocation()));
  }

  @Override
  public SqlNode visitLimitNode(Limit node, Scope context) {
    //throw if nested
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
    ResolvedFunctionCall resolvedCall = analysis.getResolvedFunctions().get(node);

    if (isLocalAggregate(node)) {
      LocalAggBuilder localAggBuilder = new LocalAggBuilder(
          new JoinPathBuilder(this.joins, this.tableMap), this.fieldNames, this.tableMap);
      ResolvedNamePath namePath = analysis.getResolvedNamePath().get(node.getArguments().get(0));
      Preconditions.checkNotNull(namePath);

      //Create new join artifact, realias condition

      String opName = node.getNamePath().get(0).getCanonical();
      SqlBasicCall call = new SqlBasicCall(resolvedCall.getFunction().getOp(),
          new SqlNode[]{new SqlResolvedIdentifier(namePath, SqlParserPos.ZERO)},
          pos.getPos(node.getLocation()));

      SqlSelect select = localAggBuilder.extractSubquery(call);
      String alias = context.getAliaser().subquery();
      SqlNode as = new SqlBasicCall(SqrlOperatorTable.AS,
          new SqlNode[]{select, new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);

      context.getAddlJoins()
          .add(new SqlJoinDeclaration(as, Optional.of(SqlLiteral.createBoolean(true, SqlParserPos.ZERO))));

      return new SqlIdentifier(List.of(alias, "tbd"), SqlParserPos.ZERO);
    }

    SqlBasicCall call = new SqlBasicCall(resolvedCall.getFunction().getOp(),
        toOperand(node.getArguments(), context), pos.getPos(node.getLocation()));

    //Convert to OVER
    //Add pk to over
    if (resolvedCall.getFunction().requiresOver()) {
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

  private boolean isLocalAggregate(FunctionCall node) {
    return analysis.getIsLocalAggregate().contains(node);
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
    ResolvedNamePath p = analysis.getResolvedNamePath().get(node);
    if (p == null) {//could be alias, etc
      return new SqlIdentifier(Arrays.stream(node.getNamePath().getNames()).map(Name::getCanonical)
          .collect(Collectors.toList()), pos.getPos(node.getLocation()));
    }

    //to-one path to be left joined
    if (p.getPath().size() > 1) {
      JoinPathBuilder joinPathBuilder = new JoinPathBuilder(this.joins, tableMap);
      SqlJoinDeclaration left = joinPathBuilder.expand(p, Optional.empty());
      context.getAddlJoins().add(left.rewriteSelfAlias(p.getAlias(),
          Optional.empty()));

      return new SqlIdentifier(List.of(left.getToTableAlias(),
          fieldNames.get(p.getPath().get(p.getPath().size() - 1))), SqlParserPos.ZERO);
    } else {

//    Preconditions.checkNotNull(p, "Could not find node {}", node);

      Field field = p.getPath().get(0);

//    Arrays.stream(node.getNamePath().getNames()).map(Name::getCanonical)
//        .collect(Collectors.toList())

      System.out.println(field + ":" + fieldNames.get(field));
      return new SqlIdentifier(List.of(p.getAlias(), fieldNames.get(field)),
          pos.getPos(node.getLocation()));
    }
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

  public SqlNode generateDistinctQuery(DistinctAssignment node) {
    ResolvedNamePath path = analysis.getResolvedNamePath().get(node.getTableNode());
    AbstractSqrlTable table = tableMap.get(path.getToTable());
    Preconditions.checkNotNull(table, "Could not find table");

    List<SqlNode> partition = node.getPartitionKeyNodes().stream().map(p -> p.accept(this, null))
        .collect(Collectors.toList());

    List<SqlNode> orderList = node.getOrder().stream().map(p -> p.accept(this, null))
        .collect(Collectors.toList());

    //Assure at least 1 order
    if (orderList.isEmpty()) {
      orderList.add(SqlNodeUtil.fieldToNode(Optional.of(path.getAlias()), table.getTimestampColumn()));
    }

    SqlNode[] operands = {
        new SqlBasicCall(SqrlOperatorTable.ROW_NUMBER, new SqlNode[]{}, SqlParserPos.ZERO),
        new SqlWindow(pos.getPos(node.getLocation()), null, null,
            new SqlNodeList(partition, SqlParserPos.ZERO),
            new SqlNodeList(orderList, SqlParserPos.ZERO),
            SqlLiteral.createBoolean(false, pos.getPos(node.getLocation())), null, null, null)};

    SqlBasicCall over = new SqlBasicCall(SqlStdOperatorTable.OVER, operands,
        pos.getPos(node.getLocation()));

    SqlBasicCall rowNumAlias = new SqlBasicCall(SqrlOperatorTable.AS,
        new SqlNode[]{over, new SqlIdentifier("_row_num", SqlParserPos.ZERO)}, SqlParserPos.ZERO);

    List<SqlNode> inner = SqlNodeUtil.toSelectList(Optional.of(path.getAlias()),
        table.getRowType(null).getFieldList());
    inner.add(rowNumAlias);

    SqlNode outerQuery = new SqlSelect(SqlParserPos.ZERO, null, new SqlNodeList(
        SqlNodeUtil.toSelectList(Optional.empty(), table.getRowType(null).getFieldList()),
        SqlParserPos.ZERO),
        new SqlSelect(SqlParserPos.ZERO, null, new SqlNodeList(inner, SqlParserPos.ZERO),
            new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
                new SqlIdentifier(tableMap.get(path.getToTable()).getNameId(), SqlParserPos.ZERO),
                new SqlNodeList(SqlParserPos.ZERO)),
                new SqlIdentifier(path.getAlias(), SqlParserPos.ZERO)}, SqlParserPos.ZERO), null,
            null, null, null, null, null, null, new SqlNodeList(SqlParserPos.ZERO)),
        new SqlBasicCall(SqrlOperatorTable.EQUALS,
            new SqlNode[]{new SqlIdentifier("_row_num", SqlParserPos.ZERO),
                SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)}, SqlParserPos.ZERO), null,
        null, null, null, null, null, new SqlNodeList(SqlParserPos.ZERO));

    return outerQuery;
  }

  protected void createParentChildJoinDeclaration(Relationship rel) {
    String alias = "t" + (++i);
    this.getJoins().put(rel,
        new SqlJoinDeclaration(createTableRef(rel.getToTable(), alias), Optional.of(createParentChildCondition(rel, alias))));
  }

  protected SqlNode createParentChildCondition(Relationship rel, String alias) {
    AbstractSqrlTable lhs =
        rel.getJoinType().equals(Relationship.JoinType.PARENT) ? tableMap.get(rel.getFromTable())
            : tableMap.get(rel.getToTable());
    AbstractSqrlTable rhs = rel.getJoinType().equals(Relationship.JoinType.PARENT) ? tableMap.get(rel.getToTable())
        : tableMap.get(rel.getFromTable());

    List<SqlNode> conditions = new ArrayList<>();
    for (String pk : lhs.getPrimaryKeys()) {
      conditions.add(new SqlBasicCall(SqrlOperatorTable.EQUALS,
          new SqlNode[]{new SqlIdentifier(List.of("_", pk), SqlParserPos.ZERO),
              new SqlIdentifier(List.of(alias, pk), SqlParserPos.ZERO)}, SqlParserPos.ZERO));
    }

    return and(conditions);
  }
  static int i = 0;

  protected SqlNode createTableRef(Table table, String alias) {
    return new SqlBasicCall(SqrlOperatorTable.AS, new SqlNode[]{new SqlTableRef(SqlParserPos.ZERO,
        new SqlIdentifier(tableMap.get(table).getNameId(), SqlParserPos.ZERO), SqlNodeList.EMPTY),
        new SqlIdentifier(alias, SqlParserPos.ZERO)}, SqlParserPos.ZERO);
  }


  @Getter
  public static class Scope {

    //todo: Should be also the trailing condition
    final List<SqlJoinDeclaration> addlJoins = new ArrayList<>();
    //    final List<SqlNode> subqueries = new ArrayList<>();
//    final List<Optional<SqlNode>> conditions = new ArrayList<>();
    final Optional<AbstractSqrlTable> parentTable;
    final boolean isNested;

    @Getter
    public Aliaser aliaser = new Aliaser();
    @Setter
    int offset = 0;
    private SqlNode join = null;
    private int PPKOffset;

    public Scope(Optional<AbstractSqrlTable> parentTable, boolean isNested) {
      this.parentTable = parentTable;
      this.isNested = isNested;
    }

    public void setPullupCondition(SqlNode join) {
      this.join = join;
    }

    public Optional<SqlNode> consumePullupCondition() {
      SqlNode tmp = this.join;
      this.join = null;
      return Optional.ofNullable(tmp);
    }

    public void setPPKOffset(int ppkOffset) {
      this.PPKOffset = ppkOffset;
    }

    public int getPPKOffset() {
      return PPKOffset;
    }
  }

  public static class Aliaser {

    public String aliasTable(String tablename) {
      return tablename.substring(0, 1).toUpperCase();
    }

    public String subquery() {
      return "s";

    }

  }
}
