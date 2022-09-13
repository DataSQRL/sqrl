package ai.datasqrl.plan.local.transpile;

import static ai.datasqrl.plan.calcite.hints.SqrlHintStrategyTable.DISTINCT_ON;
import static ai.datasqrl.plan.calcite.util.SqlNodeUtil.and;
import static org.apache.calcite.sql.SqlUtil.stripAs;

import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.plan.local.generate.Resolve.StatementOp;
import ai.datasqrl.schema.SQRLTable;
import com.google.common.collect.ArrayListMultimap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.Value;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.ExpandableTableNamespace;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.apache.flink.util.Preconditions;

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
  private final JoinBuilderFactory joinBuilderFactory;

  public Transpile(Env env, StatementOp op, TranspileOptions options) {
    this.env = env;
    this.op = op;
    this.options = options;
    this.joinBuilderFactory = () -> new JoinBuilderImpl(env, op);
  }
/*

  public void rewriteExpression(SqlNode expr, ListScope scope) {
    SqlNode newExpr = convertExpression(expr, scope);
    String fieldName = deriveAlias(expr, new TreeSet<>(), 0);
    System.out.println();
  }
*/

  public void rewriteQuery(SqlSelect select, SqlValidatorScope scope) {
    createParentPrimaryKeys(scope);

    //Before any transformation, replace group by with ordinals
    rewriteGroup(select, scope);
    rewriteOrder(select, scope);

    rewriteSelectList(select, scope);
    rewriteWhere(select, scope);

    SqlNode from = rewriteFrom(select.getFrom(), scope, Optional.empty());
    from = extraFromItems(from, scope);
    select.setFrom(from);

    rewriteHints(select, scope);
  }

  private void rewriteHints(SqlSelect select, SqlValidatorScope scope) {
    SqlNodeList hints = select.getHints();
    if (hints == null) {
      return;
    }
    List<SqlNode> list = hints.getList();

    for (int i = 0; i < list.size(); i++) {
      SqlHint hint = (SqlHint) list.get(i);
      if (hint.getName().equals(DISTINCT_ON)) {
        SqlHint newHint = rewriteDistinctHint(select, hint, scope);
        hints.set(i, newHint);
      }
    }

    select.setHints(hints);
  }

  private SqlHint rewriteDistinctHint(SqlSelect select, SqlHint hint, SqlValidatorScope scope) {
    List<SqlNode> asIdentifiers = hint.getOptionList().stream()
        .map(o -> new SqlIdentifier(List.of(o.split("\\.")), hint.getParserPosition()))
        .collect(Collectors.toList());

    List<SqlNode> partitionKeyIndices = getSelectListindex(select, asIdentifiers, 0).stream()
        .map(e -> new SqlIdentifier(((SqlNumericLiteral) e).getValue().toString(),
            SqlParserPos.ZERO)).collect(Collectors.toList());

    SqlHint newHint = new SqlHint(hint.getParserPosition(),
        new SqlIdentifier(hint.getName(), hint.getParserPosition()),
        new SqlNodeList(partitionKeyIndices, hint.getParserPosition()), hint.getOptionFormat());
    return newHint;
  }

  private void createParentPrimaryKeys(SqlValidatorScope scope) {
    Optional<SQRLTable> context = Optional.empty();//op.getSqrlValidator().getContextTable(scope);
    List<NamedKey> nodes = new ArrayList<>();
    if (context.isPresent()) {
      TableWithPK t = env.getTableMap().get(context.get());
      //self table could be aliased
      String contextAlias = op.getSqrlValidator().getContextAlias();
      for (String key : t.getPrimaryKeys()) {
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
    ExpressionRewriter expressionRewriter = new ExpressionRewriter(scope, env, op);
    SqlNode rewritten = expr.accept(expressionRewriter);

    this.inlineAgg.putAll(scope, expressionRewriter.getInlineAggResults());
    this.toOne.putAll(scope, expressionRewriter.getToOneResults());

    return rewritten;
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

    List<SqlNode> mutableGroupItems = new ArrayList<>();
    extraPPKItems(scope, mutableGroupItems);

    //Find the new rewritten select items, replace with alias
    SqlNodeList group = select.getGroup() == null ? SqlNodeList.EMPTY : select.getGroup();
    List<SqlNode> ordinals = getSelectListindex(select, group.getList(),
        mutableGroupItems.size());
    mutableGroupItems.addAll(ordinals);

    if (!mutableGroupItems.isEmpty()) {
      select.setGroupBy(new SqlNodeList(mutableGroupItems,
          select.getGroup() == null ? select.getParserPosition()
              : select.getGroup().getParserPosition()));
    }
  }

  private List<SqlNode> getSelectListindex(SqlSelect select, List<SqlNode> toCheck, int offset) {
    List<SqlNode> indicies = new ArrayList<>();
    outer:
    for (SqlNode groupNode : toCheck) {
      SelectScope selectScope = op.getSqrlValidator().getRawSelectScope(select);
      List<SqlNode> list = selectScope.getExpandedSelectList();
      for (int i = 0; i < list.size(); i++) {
        SqlNode selectNode = list.get(i);
        switch (selectNode.getKind()) {
          case AS:
            SqlCall call = (SqlCall) selectNode;
            if (groupNode.equalsDeep(call.getOperandList().get(0), Litmus.IGNORE)
                || groupNode.equalsDeep(call.getOperandList().get(1), Litmus.IGNORE)) {
              indicies.add(SqlLiteral.createExactNumeric(Long.toString(i + offset),
                  groupNode.getParserPosition()));
              continue outer;
            }
            break;
          default:
            if (groupNode.equalsDeep(selectNode, Litmus.IGNORE) ||
                (groupNode instanceof SqlIdentifier && selectScope.fullyQualify((SqlIdentifier) groupNode)
                    .identifier.equalsDeep(selectNode, Litmus.IGNORE))
            ) {
              indicies.add(SqlLiteral.createExactNumeric(Long.toString(i + offset),
                  groupNode.getParserPosition()));
              continue outer;
            }
            break;
        }
      }
      throw new RuntimeException("Could not find in select list " + groupNode);
    }
    return indicies;
  }

  private void extraPPKItems(SqlValidatorScope scope, List<SqlNode> groupItems) {
    List<NamedKey> names = this.generatedKeys.get(scope);
    for (NamedKey key : names) {
      groupItems.add(new SqlIdentifier(List.of(key.getName()), SqlParserPos.ZERO));
    }
  }

  private void rewriteOrder(SqlSelect select, SqlValidatorScope scope) {
    //If no orders, exit
    if (select.getOrderList() == null || select.getOrderList().getList().isEmpty()) {
      return;
    }

    List<SqlNode> mutableOrders = new ArrayList<>();
    extraPPKItems(scope, mutableOrders);
    List<SqlNode> cleaned = select.getOrderList().getList().stream().map(o -> {
      if (o.getKind() == SqlKind.DESCENDING || o.getKind() == SqlKind.NULLS_FIRST
          || o.getKind() == SqlKind.NULLS_LAST) {
        //is DESCENDING, nulls first, nulls last
        return ((SqlCall) o).getOperandList().get(0);
      }
      return o;
    }).map(o -> op.getSqrlValidator().expandOrderExpr(select, o)).collect(Collectors.toList());

    //If aggregating, replace each select item with ordinal
    if (op.getSqrlValidator().isAggregate(select)) {
      List<SqlNode> ordinals = getSelectListindex(select, cleaned, mutableOrders.size());

      //Readd w/ order
      for (int i = 0; i < select.getOrderList().size(); i++) {
        SqlNode o = select.getOrderList().get(i);
        if (o.getKind() == SqlKind.DESCENDING || o.getKind() == SqlKind.NULLS_FIRST
            || o.getKind() == SqlKind.NULLS_LAST) {
          SqlCall call = ((SqlCall) o);
          call.setOperand(0, ordinals.get(i));
          mutableOrders.add(call);
        } else {
          mutableOrders.add(ordinals.get(i));
        }
      }

      select.setOrderBy(new SqlNodeList(mutableOrders, select.getOrderList().getParserPosition()));
      return;
    } else {
      //Otherwise, we want to check the select list first for ordinal, but if its not there then
      // we expand it
      List<SqlNode> expanded = op.getSqrlValidator().getRawSelectScope(select).getExpandedSelectList();
      outer:
      for (SqlNode orderNode : cleaned) {
        //look for order in select list
        if (options.orderToOrdinals) {
          for (int i = 0; i < expanded.size(); i++) {
            SqlNode selectItem = expanded.get(i);
            selectItem = stripAs(selectItem);
            //Found an ordinal
            if (orderNode.equalsDeep(selectItem, Litmus.IGNORE)) {
              SqlNode ordinal = SqlLiteral.createExactNumeric(
                  Long.toString(i + mutableOrders.size() + 1), SqlParserPos.ZERO);
              if (orderNode.getKind() == SqlKind.DESCENDING
                  || orderNode.getKind() == SqlKind.NULLS_FIRST
                  || orderNode.getKind() == SqlKind.NULLS_LAST) {
                SqlCall call = ((SqlCall) orderNode);
                call.setOperand(0, ordinal);
                mutableOrders.add(call);
              } else {
                mutableOrders.add(ordinal);
              }
              continue outer;
            }
          }
        }
        //otherwise, process it
        SqlNode ordinal = convertExpression(orderNode, scope);
        if (orderNode.getKind() == SqlKind.DESCENDING || orderNode.getKind() == SqlKind.NULLS_FIRST
            || orderNode.getKind() == SqlKind.NULLS_LAST) {
          SqlCall call = ((SqlCall) orderNode);
          call.setOperand(0, ordinal);
          mutableOrders.add(call);
        } else {
          mutableOrders.add(ordinal);
        }
      }
    }

    if (!mutableOrders.isEmpty()) {
      select.setOrderBy(new SqlNodeList(mutableOrders, select.getOrderList().getParserPosition()));
    }
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
    final SqlValidatorNamespace fromNamespace = op.getSqrlValidator().getNamespace(id).resolve();

    if (fromNamespace.getNode() != null) {
      return rewriteFrom(fromNamespace.getNode(), scope, Optional.empty());
    }

    if (id.names.size() == 1 && id.names.get(0).equalsIgnoreCase("_")) {
      return rewriteTable(id, scope);
    }

    if (fromNamespace instanceof ExpandableTableNamespace) {
      ExpandableTableNamespace tn = (ExpandableTableNamespace) fromNamespace;

      TablePath tablePath = tn.createTablePath(alias);

      SqlJoinDeclaration declaration = JoinBuilderImpl.expandPath(tablePath, false,
          joinBuilderFactory);
      declaration.getPullupCondition().ifPresent(pullupConditions::push);
      return declaration.getJoinTree();
    } else {
      //just do a simple mapping from table
      SQRLTable baseTable = fromNamespace.getTable().unwrap(SQRLTable.class);
      TableWithPK basePkTable = env.getTableMap().get(baseTable);
      return SqlNodeBuilder.createTableNode(basePkTable, Util.last(id.names));
    }
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
