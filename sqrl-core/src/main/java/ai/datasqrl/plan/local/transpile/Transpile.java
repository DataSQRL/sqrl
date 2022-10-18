package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.calcite.hints.TopNHint;
import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Resolve.StatementKind;
import ai.datasqrl.plan.local.generate.Resolve.StatementOp;
import ai.datasqrl.schema.SQRLTable;
import com.google.common.collect.ArrayListMultimap;
import lombok.Value;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.*;
import org.apache.calcite.util.Util;
import org.apache.flink.util.Preconditions;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static ai.datasqrl.plan.calcite.util.SqlNodeUtil.and;

public class Transpile {

  private final Stack<SqlNode> pullupConditions = new Stack<>();
  private final ArrayListMultimap<SqlValidatorScope, InlineAggExtractResult> inlineAgg =
      ArrayListMultimap.create();
  private final ArrayListMultimap<SqlValidatorScope, SqlJoinDeclaration> toOne =
      ArrayListMultimap.create();
  private final Map<SqlValidatorScope, List<NamedKey>> generatedKeys = new HashMap<>();

  private final Env env;
  private final StatementOp op;
  private final TranspileOptions options;
  Optional<SQRLTable> context;
//  private final JoinBuilderFactory joinBuilderFactory;

  public Transpile(Env env, StatementOp op, Optional<SQRLTable> context, TranspileOptions options) {
    this.env = env;
    this.op = op;
    this.options = options;
    this.context = context;
//    this.joinBuilderFactory = () -> new JoinBuilderImpl(env, op);
  }

  public void rewriteQuery(SqlSelect select, SqlValidatorScope scope) {
    createParentPrimaryKeys(scope);

    validateNoPaths(select.getGroup(), scope);
    validateNoPaths(select.getOrderList(), scope);

    //Before any transformation, replace group by with ordinals
    if (!select.isDistinct() &&
        !(isNested(op) && select.getFetch() != null)) { //we add this to the hint instead of these keywords
      rewriteGroup(select, scope);
      rewriteOrder(select, scope);
    }

    rewriteSelectList(select, scope);
    rewriteWhere(select, scope);

//    SqlNode from = rewriteFrom(select.getFrom(), scope, Optional.empty());
//    from = extraFromItems(from, scope);
//    select.setFrom(from);

    if (select.isDistinct() && select.getGroup() != null) {
      throw new RuntimeException("SELECT DISTINCT with a GROUP not yet supported");
    }

    if (select.isDistinct()) {
      rewriteDistinctingHint(select, scope, (ppkNode) ->
          TopNHint.createSqlHint(TopNHint.Type.SELECT_DISTINCT, ppkNode, SqlParserPos.ZERO));
    } else if (isNested(op) && select.getFetch() != null) {
      rewriteDistinctingHint(select, scope, (ppkNode) ->
              TopNHint.createSqlHint(TopNHint.Type.TOP_N, ppkNode, SqlParserPos.ZERO));
    } else if (op.getStatementKind() == StatementKind.DISTINCT_ON) {
      rewriteDistinctOnHint(select);
    }
    rewriteHints(select, scope);
  }

  private void rewriteDistinctOnHint(SqlSelect select) {
    CalciteUtil.wrapSelectInProject(select);
    SqlSelect inner = (SqlSelect)select.getFrom();
    //pull up hints
    select.setHints(inner.getHints());
    inner.setHints(new SqlNodeList(SqlParserPos.ZERO));
  }

  private boolean isNested(StatementOp op) {
    return context.isPresent();
  }

  private void rewriteHints(SqlSelect select, SqlValidatorScope scope) {
    SqlNodeList hints = select.getHints();
    if (hints == null) {
      return;
    }
    List<SqlNode> list = hints.getList();

    for (int i = 0; i < list.size(); i++) {
      SqlHint hint = (SqlHint) list.get(i);
      if (hint.getName().equals(TopNHint.Type.DISTINCT_ON.name())) {
        SqlHint newHint = rewriteDistinctHint(select, hint, scope);
        hints.set(i, newHint);
      }
    }

    select.setHints(hints);
  }

  /**
   * SELECT DISTINCT `_`.`customerid` AS `_pk_1`, `o`.`id` AS `id`, `o`.`time` AS `time`
   * FROM `customer$12` AS `_`
   * DEFAULT JOIN `orders$6` AS `o` AS `o` ON `_`.`customerid` = `o`.`customerid`
   * ORDER BY `_`.`customerid`, `o`.`time` DESC
   * FETCH NEXT 10 ROWS ONLY;
   *
   *  SELECT +`SELECT_DISTINCT`(`0`)+ `_pk_1`, `id`, `time`
   * FROM (SELECT `_`.`customerid` AS `_pk_1`, `o`.`id` AS `id`, `o`.`time` AS `time`
   *       FROM `customer$12` AS `_`
   *       DEFAULT JOIN `orders$6` AS `o` AS `o` ON `_`.`customerid` = `o`.`customerid` )
   *       ORDER BY `_`.`customerid`, `o`.`time` DESC
   *       FETCH NEXT 10 ROWS ONLY)
   *
   */
  private void rewriteDistinctingHint(SqlSelect select, SqlValidatorScope scope, Function<SqlNodeList, SqlHint> createFnc) {
    CalciteUtil.removeKeywords(select);
    SqlNodeList innerSelectList = select.getSelectList();
    CalciteUtil.wrapSelectInProject(select);

    List<SqlNode> innerPPKNodes = getPPKNodes(scope);
    List<SqlNode> ppkNodeIndex = mapIndexOfNodeList(innerSelectList, innerPPKNodes);
    SqlNodeList ppkNode = new SqlNodeList(ppkNodeIndex, SqlParserPos.ZERO);
    SqlHint selectDistinctHint = createFnc.apply(ppkNode);
    CalciteUtil.setHint(select, selectDistinctHint);
  }

  private List<SqlNode> mapIndexOfNodeList(SqlNodeList list,
      List<SqlNode> innerPPKNodes) {
    List<SqlNode> index = new ArrayList<>();
    for (int i = 0; i < list.getList().size(); i++) {
      if (CalciteUtil.deepContainsNodeName(innerPPKNodes, list.get(i))) {
        SqlIdentifier identifier = new SqlIdentifier(List.of(Integer.toString(i)), SqlParserPos.ZERO);
        index.add(identifier);
      }
    }
    return index;
  }

  private SqlHint rewriteDistinctHint(SqlSelect select, SqlHint hint, SqlValidatorScope scope) {
    List<SqlNode> options = CalciteUtil.getHintOptions(hint);

    appendUniqueToSelectList(select, options, scope);

    List<SqlNode> partitionKeyIndices = getSelectListIndex(select, options, 0, scope).stream()
        .map(e -> new SqlIdentifier(((SqlNumericLiteral) e).getValue().toString(),
            SqlParserPos.ZERO)).collect(Collectors.toList());

    SqlHint newHint = new SqlHint(hint.getParserPosition(),
        new SqlIdentifier(hint.getName(), hint.getParserPosition()),
        new SqlNodeList(partitionKeyIndices, hint.getParserPosition()), hint.getOptionFormat());
    return newHint;
  }

  private void appendUniqueToSelectList(SqlSelect select, List<SqlNode> options,
      SqlValidatorScope scope) {
    for (int i = 0; i < options.size(); i++) {
      SqlNode option = options.get(i);
      if (!expressionInSelectList(select, option, scope)) {
        SqlCall call = SqlStdOperatorTable.AS.createCall(option.getParserPosition(),
            List.of(
                option,
                new SqlIdentifier("_option_" + i, SqlParserPos.ZERO)));
        CalciteUtil.appendSelectListItem(select, call);
      }
    }
  }

  private boolean expressionInSelectList(SqlSelect select, SqlNode exp, SqlValidatorScope scope) {
    for (SqlNode selectItem : select.getSelectList()) {
      if (CalciteUtil.selectListExpressionEquals(selectItem, exp, scope)) {
        return true;
      }
    }
    return false;
  }

  private void createParentPrimaryKeys(SqlValidatorScope scope) {

    List<NamedKey> nodes = new ArrayList<>();
    if (context.isPresent()) {
      TableWithPK t = env.getTableMap().get(context.get());
      //self table could be aliased
      String contextAlias = op.getSqrlValidator().getContextAlias();
      for (String key : t.getPrimaryKeyNames()) {
        String pk = env.getAliasGenerator().generatePK();
        nodes.add(
            new NamedKey(pk, new SqlIdentifier(List.of(contextAlias, key), SqlParserPos.ZERO)));
      }
    }
    this.generatedKeys.put(scope, nodes);
  }

  private void rewriteSelectList(SqlSelect select, SqlValidatorScope scope) {
    List<SqlNode> selectList = op.getSqrlValidator().getRawSelectScope(select).getExpandedSelectList();

    List<String> fieldNames = new ArrayList<>();
    final List<SqlNode> exprs = new ArrayList<>();
    final Collection<String> aliasSet = new TreeSet<>();

    // Project any system/nested fields. (Must be done before regular select items,
    // because offsets may be affected.)
    extraSelectItems(exprs, fieldNames, scope);

    // Project select clause.
    int i = -1;
    for (SqlNode expr : selectList) {
      ++i;
      exprs.add(convertExpression(expr, scope));
      fieldNames.add(deriveAlias(expr, aliasSet, i));
    }

    List<String> newFieldNames = SqlValidatorUtil.uniquify(fieldNames, false);

    List<SqlNode> newSelect = IntStream.range(0, exprs.size())
        .mapToObj(j -> SqlNodeBuilder.as(exprs.get(j), newFieldNames.get(j)))
        .collect(Collectors.toList());

    select.setSelectList(new SqlNodeList(newSelect, select.getSelectList().getParserPosition()));
  }

  private SqlNode convertExpression(SqlNode expr, SqlValidatorScope scope) {
//    ExpressionRewriter expressionRewriter = new ExpressionRewriter(scope, env, op);
//    SqlNode rewritten = expr.accept(expressionRewriter);
//
//    this.inlineAgg.putAll(scope, expressionRewriter.getInlineAggResults());
//    this.toOne.putAll(scope, expressionRewriter.getToOneResults());
//
    return expr;
  }

  private String deriveAlias(final SqlNode node, Collection<String> aliases, final int ordinal) {
    String alias = op.getSqrlValidator().deriveAlias(node, ordinal);
    if ((alias == null) || aliases.contains(alias)) {
      String aliasBase = (alias == null) ? "EXPR$" : alias;
      for (int j = 0; ; j++) {
        alias = aliasBase + j;
        if (!aliases.contains(alias)) {
          break;
        }
      }
    }
    aliases.add(alias);
    return alias;
  }

  private void extraSelectItems(List<SqlNode> exprs, List<String> fieldNames,
      SqlValidatorScope scope) {
    //Must uniquely add names relative to the existing select list names
    // e.g. (_pk1, _pk2)

    List<NamedKey> ppk = generatedKeys.get(scope);
    if (ppk != null) {
      for (NamedKey key : ppk) {
        exprs.add(key.getNode());
        fieldNames.add(key.getName());
      }
    }
  }

  private void rewriteWhere(SqlSelect select, SqlValidatorScope scope) {
    if (select.getWhere() == null) {
      return;
    }
    SqlNode rewritten = convertExpression(select.getWhere(), scope);
    select.setWhere(rewritten);
  }

  private void rewriteGroup(SqlSelect select, SqlValidatorScope scope) {
    if (!op.getSqrlValidator().isAggregate(select)) {
      Preconditions.checkState(select.getGroup() == null);
      return;
    }

    List<SqlNode> ppkNodes = getPPKNodes(scope);
    CalciteUtil.prependGroupByNodes(select, ppkNodes);
  }

  /**
   * Validate there are no path expressions, group by and order should use select list aliases
   * or create a hidden one if one is not found
   */
  private void validateNoPaths(SqlNodeList nodeList, SqlValidatorScope scope) {
    if (nodeList == null) {
      return;
    }
    for (SqlNode node : nodeList) {
      validateNoPaths(node, scope);
    }
  }

  private void validateNoPaths(SqlNode node, SqlValidatorScope scope) {
    PathDetectionVisitor pathDetectionVisitor = new PathDetectionVisitor(scope);
    node.accept(pathDetectionVisitor);
    if (pathDetectionVisitor.hasPath()) {
      throw new RuntimeException("Node cannot contain a path, use select alias: " + node);
    }
  }

  private List<SqlNode> getSelectListIndex(SqlSelect select, List<SqlNode> operands, int offset,
      SqlValidatorScope scope) {
    List<SqlNode> nodeList = new ArrayList<>();
    for (SqlNode operand : operands) {
      int index = getIndexOfExpression(select, operand, scope);
      if (index == -1) {
        throw new RuntimeException("Could not find expression in select list: " + operand);
      }

      nodeList.add(SqlLiteral.createExactNumeric(Long.toString(index + offset),
          operand.getParserPosition()));
    }

    return nodeList;
  }

  private int getIndexOfExpression(SqlSelect select, SqlNode operand, SqlValidatorScope scope) {
    List<SqlNode> list = select.getSelectList().getList();
    for (int i = 0; i < list.size(); i++) {
      SqlNode selectItem = list.get(i);
      if (CalciteUtil.selectListExpressionEquals(selectItem, operand, scope)) {
        return i;
      }
    }
    return -1;
  }

  private List<SqlNode> getPPKNodes(SqlValidatorScope scope) {
    List<NamedKey> names = this.generatedKeys.get(scope);
    return names.stream()
        .map(key -> key.node)
        .collect(Collectors.toList());
  }

  private void rewriteOrder(SqlSelect select, SqlValidatorScope scope) {
    //If no orders, exit
    if (select.getOrderList() == null || select.getOrderList().getList().isEmpty()) {
      return;
    }

    List<SqlNode> ppkNodes = getPPKNodes(scope);
    CalciteUtil.prependOrderByNodes(select, ppkNodes);
  }

  SqlNode rewriteFrom(SqlNode from, SqlValidatorScope scope, Optional<String> aliasOpt) {
    final SqlCall call;

    switch (from.getKind()) {
      case AS:
        call = (SqlCall) from;
        SqlNode firstOperand = call.operand(0);
        String alias = Util.last(((SqlIdentifier) call.getOperandList().get(1)).names);
        SqlNode newFrom = rewriteFrom(firstOperand, scope, Optional.of(alias));
        //always preserve alias
        call.setOperand(0, newFrom);
        break;
      case TABLE_REF:
        //todo: fix  drops table hints
        SqlTableRef ref = ((SqlTableRef)from);
        from = rewriteFrom(ref.getOperandList().get(0), scope, aliasOpt);
        break;
      case IDENTIFIER:
        from = convertTableName((SqlIdentifier) from, aliasOpt
                  .orElse(Util.last(((SqlIdentifier) from).names)),
            scope);
        break;
      case JOIN:
        //from gets reassigned instead of replaced
        rewriteJoin((SqlJoin) from, scope);
        break;
      case SELECT:
        SqlValidatorScope subScope = op.getSqrlValidator().getFromScope((SqlSelect) from);
        rewriteQuery((SqlSelect) from, subScope);
        break;
      case INTERSECT:
      case EXCEPT:
      case UNION:
        throw new RuntimeException("TBD");
    }

    return from;
  }

  private SqlNode convertTableName(SqlIdentifier id, String alias, SqlValidatorScope scope) {
//    final SqlValidatorNamespace fromNamespace = op.getSqrlValidator().getNamespace(id).resolve();
//
//    if (fromNamespace.getNode() != null) {
//      return rewriteFrom(fromNamespace.getNode(), scope, Optional.empty());
//    }
//
//    if (id.names.size() == 1 && id.names.get(0).equalsIgnoreCase("_")) {
//      return rewriteTable(id, scope);
//    }
//
//    if (fromNamespace instanceof ExpandableTableNamespace) {
//      ExpandableTableNamespace tn = (ExpandableTableNamespace) fromNamespace;
//
//      TablePath tablePath = tn.createTablePath(alias);
//
//      SqlJoinDeclaration declaration = JoinBuilderImpl.expandPath(tablePath, false,
//          joinBuilderFactory);
//      declaration.getPullupCondition().ifPresent(pullupConditions::push);
//      return declaration.getJoinTree();
//    } else {
//      //just do a simple mapping from table
//      SQRLTable baseTable = fromNamespace.getTable().unwrap(SQRLTable.class);
//      TableWithPK basePkTable = env.getTableMap().get(baseTable);
//      return SqlNodeBuilder.createTableNode(basePkTable, Util.last(id.names));
//    }
    return null;
  }

  private SqlNode extraFromItems(SqlNode from, SqlValidatorScope scope) {
    List<InlineAggExtractResult> inlineAggs = inlineAgg.get(scope);
    for (InlineAggExtractResult agg : inlineAggs) {
      from = SqlNodeBuilder.createJoin(JoinType.LEFT, from, agg.getQuery(), agg.getCondition());
    }

    List<SqlJoinDeclaration> joinDecs = toOne.get(scope);
    for (SqlJoinDeclaration agg : joinDecs) {
      from = SqlNodeBuilder.createJoin(JoinType.LEFT, from, agg.getJoinTree(),
          agg.getPullupCondition().orElse(SqlLiteral.createBoolean(true, SqlParserPos.ZERO)));
    }

    return from;
  }

  private SqlNode rewriteTable(SqlIdentifier id, SqlValidatorScope scope) {
    //Expand
    op.getSqrlValidator().getNamespace(id).resolve();
    SqlValidatorNamespace ns = op.getSqrlValidator().getNamespace(id).resolve();
    SQRLTable table = ns.getTable().unwrap(SQRLTable.class);
    TableWithPK t = env.getTableMap().get(table);

    return SqlNodeBuilder.as(new SqlIdentifier(t.getNameId(), SqlParserPos.ZERO),
        Util.last(id.names));
  }

  private void rewriteJoin(SqlJoin join, SqlValidatorScope rootScope) {
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    final SqlValidatorScope leftScope = Util.first(op.getSqrlValidator().getJoinScope(left),
        ((DelegatingScope) rootScope).getParent());

    final SqlValidatorScope rightScope = Util.first(op.getSqrlValidator().getJoinScope(right),
        ((DelegatingScope) rootScope).getParent());

    SqlNode l = rewriteFrom(left, leftScope, Optional.empty());
    join.setLeft(l);
    SqlNode r = rewriteFrom(right, rightScope, Optional.empty());
    join.setRight(r);

    Optional<SqlNode> condition;
    if (!pullupConditions.isEmpty()) {
      condition = Optional.of(pullupConditions.pop());
    } else {
      condition = Optional.empty();
    }

    final JoinConditionType conditionType = join.getConditionType();
    if (join.isNatural()) {
      //todo:
      // Need to see if there is an extra join condition I need to append and then convert
//        condition = convertNaturalCondition(op.getSqrlValidator().getNamespace(left),
//            op.getSqrlValidator().getNamespace(right));
//        rightRel = tempRightRel;
    } else {
      switch (conditionType) {
        case NONE:

          SqlNode newNoneCondition = condition.orElse(
              SqlLiteral.createBoolean(true, SqlParserPos.ZERO));
          join.setOperand(2, SqlLiteral.createSymbol(JoinType.INNER, SqlParserPos.ZERO));
          join.setOperand(4, SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO));
          join.setOperand(5, newNoneCondition);
          break;
        case USING:
          //todo: Using
//            condition = convertUsingCondition(join,
//                op.getSqrlValidator().getNamespace(left),
//                op.getSqrlValidator().getNamespace(right));
//            rightRel = tempRightRel;
          break;
        case ON:
          SqlNode newOnCondition = condition.map(c -> and(join.getCondition(), c))
              .orElse(join.getCondition());
          join.setOperand(5, newOnCondition);
          break;
        default:
          throw Util.unexpected(conditionType);
      }
    }
  }

  @Value
  static class NamedKey {

    String name;
    SqlNode node;
  }
}
