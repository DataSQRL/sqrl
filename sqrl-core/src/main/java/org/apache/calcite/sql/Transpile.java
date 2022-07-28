package org.apache.calcite.sql;

import static ai.datasqrl.plan.local.generate.node.util.SqlNodeUtil.and;
import static org.apache.calcite.sql.SqlUtil.stripAs;

import ai.datasqrl.plan.calcite.sqrl.hints.SqrlHintStrategyTable;
import ai.datasqrl.plan.calcite.sqrl.table.TableWithPK;
import ai.datasqrl.plan.local.generate.QueryGenerator.FieldNames;
import ai.datasqrl.schema.ScriptTable;
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
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql.validate.SqrlValidatorImpl;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;
import org.apache.flink.util.Preconditions;

@AllArgsConstructor
public class Transpile {

  final Stack<SqlNode> pullupConditions = new Stack();
  final Map<SqlNode, SqlNode> replaced = new HashMap<>();
  private final ArrayListMultimap<SqlValidatorScope, InlineAggExtractResult> inlineAgg =
      ArrayListMultimap.create();
  private final ArrayListMultimap<SqlValidatorScope, JoinDeclaration> toOne =
      ArrayListMultimap.create();
  private final Map<SqlValidatorScope, List<SqlNode>> parentPrimaryKeys = new HashMap<>();
  private final Map<SqlValidatorScope, List<String>> parentPrimaryKeyNames = new HashMap<>();
  SqrlValidatorImpl validator;
  TableMapperImpl tableMapper;
  UniqueAliasGeneratorImpl uniqueAliasGenerator;
  JoinDeclarationContainerImpl joinDecs;
  SqlNodeBuilderImpl sqlNodeBuilder;
  JoinBuilderFactory joinBuilderFactory;
  FieldNames names;

  public void rewriteQuery(SqlSelect select, SqlValidatorScope scope) {
    createParentPrimaryKeys(scope);

    //Before any transformation, replace group by with ordinals
    rewriteGroup(select, scope);
    rewriteOrder(select, scope);

    rewriteSelectList(select, scope);
    rewriteWhere(select, scope);

    SqlNode from = rewriteFrom(select.getFrom(), scope);
    from = extraFromItems(from, scope);
    select.setFrom(from);

    rewriteHints(select, scope);
  }

  private void rewriteHints(SqlSelect select, SqlValidatorScope scope) {
    SqlNodeList hints = select.getHints();
    List<SqlNode> list = hints.getList();

    for (int i = 0; i < list.size(); i++) {
      SqlHint hint = (SqlHint) list.get(i);
      if (hint.getName().equals(SqrlHintStrategyTable.DISTINCT_ON_HINT_NAME)) {
        SqlHint newHint = rewriteDistinctHint(select, hint, scope);
        hints.set(i, newHint);
      }
    }

    select.setHints(hints);
  }

  private SqlHint rewriteDistinctHint(SqlSelect select, SqlHint hint, SqlValidatorScope scope) {
    List<SqlNode> asIdentifiers = hint.getOptionList().stream()
        .map(o ->new SqlIdentifier(List.of(o.split("\\.")), hint.pos))
        .collect(Collectors.toList());

    List<SqlNode> partitionKeyIndices = getSelectListOrdinals(select, asIdentifiers, 0)
        .stream().map(e->new SqlIdentifier(((SqlNumericLiteral)e).getValue().toString(), SqlParserPos.ZERO))
        .collect(Collectors.toList());

    SqlHint newHint = new SqlHint(hint.getParserPosition(),
        new SqlIdentifier(hint.getName(), hint.pos),
        new SqlNodeList(partitionKeyIndices, hint.pos), hint.getOptionFormat());
    return newHint;
  }

  private void createParentPrimaryKeys(SqlValidatorScope scope) {
    Optional<ScriptTable> context = validator.getContextTable(scope);
    List<SqlNode> nodes = new ArrayList<>();
    List<String> names = new ArrayList<>();
    if (context.isPresent()) {
      TableWithPK t = tableMapper.getTable(context.get());
      //self table could be aliased
      String contextAlias = validator.getContextAlias();
      for (String key : t.getPrimaryKeys()) {
        String pk = uniqueAliasGenerator.generatePK();
        nodes.add(new SqlIdentifier(List.of(contextAlias, key), SqlParserPos.ZERO));
        names.add(pk);
      }
    }
    this.parentPrimaryKeys.put(scope, nodes);
    this.parentPrimaryKeyNames.put(scope, names);
  }

  private void rewriteSelectList(SqlSelect select, SqlValidatorScope scope) {
    Optional<ScriptTable> ctx = ((SqrlValidatorImpl) scope.getValidator()).getCtx();
//    SqlNodeList selectList = select.getSelectList();

    List<SqlNode> selectList = validator.getRawSelectScope(select).getExpandedSelectList();

    List<String> fieldNames = new ArrayList<>();
    final List<SqlNode> exprs = new ArrayList<>();
    final Collection<String> aliasSet = new TreeSet<>();

    // Project any system/nested fields. (Must be done before regular select items,
    // because offsets may be affected.)
    extraSelectItems(
        exprs,
        fieldNames,
        scope);

    // Project select clause.
    int i = -1;
    for (SqlNode expr : selectList) {
      ++i;
      exprs.add(convertExpression(expr, scope));
      fieldNames.add(deriveAlias(expr, aliasSet, i));
    }

    List<String> newFieldNames = SqlValidatorUtil.uniquify(fieldNames, false);

    List<SqlNode> newSelect = IntStream.range(0, exprs.size())
        .mapToObj(j -> sqlNodeBuilder.as(exprs.get(j), newFieldNames.get(j)))
        .collect(Collectors.toList());

    select.setSelectList(new SqlNodeList(newSelect, select.getSelectList().getParserPosition()));
  }

  private SqlNode convertExpression(SqlNode expr, SqlValidatorScope scope) {
    ExpressionRewriter expressionRewriter = new ExpressionRewriter(scope,
        tableMapper, uniqueAliasGenerator, joinDecs, sqlNodeBuilder, names, this);
    SqlNode rewritten = expr.accept(expressionRewriter);

    this.inlineAgg.putAll(scope, expressionRewriter.getInlineAggResults());
    this.toOne.putAll(scope, expressionRewriter.getToOneResults());

    return rewritten;
  }

  private String deriveAlias(
      final SqlNode node,
      Collection<String> aliases,
      final int ordinal) {
    String alias = validator.deriveAlias(node, ordinal);
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

  private void extraSelectItems(List<SqlNode> exprs,
      List<String> fieldNames, SqlValidatorScope scope) {
    //Must uniquely add names relative to the existing select list names
    // e.g. (_pk1, _pk2)

    List<SqlNode> ppk = parentPrimaryKeys.get(scope);
    if (ppk != null) {
      List<String> keyNames = parentPrimaryKeyNames.get(scope);
      for (int i = 0; i < ppk.size(); i++) {
        exprs.add(ppk.get(i));
        fieldNames.add(keyNames.get(i));
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
    if (!validator.isAggregate(select)) {
      Preconditions.checkState(select.groupBy == null);
      return;
    }

    List<SqlNode> mutableGroupItems = new ArrayList<>();
    extraPPKItems(select, scope, mutableGroupItems);

    //Find the new rewritten select items, replace with alias
    SqlNodeList group = select.getGroup() == null ? SqlNodeList.EMPTY : select.getGroup();
    List<SqlNode> ordinals = getSelectListOrdinals(select, group.getList(),
        mutableGroupItems.size());
    mutableGroupItems.addAll(ordinals);

    if (!mutableGroupItems.isEmpty()) {
      select.setGroupBy(new SqlNodeList(mutableGroupItems,select.getGroup() == null ?
          select.getParserPosition() : select.getGroup().getParserPosition()));
    }
  }

  private List<SqlNode> getSelectListOrdinals(SqlSelect select, List<SqlNode> toCheck, int offset) {
    List<SqlNode> ordinals = new ArrayList<>();
    outer:
    for (SqlNode groupNode : toCheck) {
      List<SqlNode> list = validator.getRawSelectScope(select).getExpandedSelectList();
      for (int i = 0; i < list.size(); i++) {
        SqlNode selectNode = list.get(i);
        switch (selectNode.getKind()) {
          case AS:
            SqlCall call = (SqlCall) selectNode;
            if (groupNode.equalsDeep(call.getOperandList().get(0), Litmus.IGNORE) ||
                groupNode.equalsDeep(call.getOperandList().get(1), Litmus.IGNORE)) {
              ordinals.add(SqlLiteral.createExactNumeric(Long.toString(i + offset + 1),
                  groupNode.getParserPosition()));
              continue outer;
            }
            break;
          default:
            if (groupNode.equalsDeep(selectNode, Litmus.IGNORE)) {
              ordinals.add(SqlLiteral.createExactNumeric(Long.toString(i + offset + 1),
                  groupNode.getParserPosition()));
              continue outer;
            }
            break;
        }
      }
      throw new RuntimeException("Could not find in select list " + groupNode);
    }
    return ordinals;
  }

  private void extraPPKItems(SqlSelect select, SqlValidatorScope scope,
      List<SqlNode> groupItems) {
    List<String> names = this.parentPrimaryKeyNames.get(scope);
    for (String name : names) {
      groupItems.add(new SqlIdentifier(List.of(name), SqlParserPos.ZERO));
    }
  }

  private void rewriteOrder(SqlSelect select, SqlValidatorScope scope) {
    //If no orders, exit
    if (select.getOrderList() == null || select.getOrderList().getList().isEmpty()) {
      return;
    }

    List<SqlNode> mutableOrders = new ArrayList<>();
    extraPPKItems(select, scope, mutableOrders);
    List<SqlNode> cleaned = select.getOrderList().getList().stream()
        .map(o -> {
          if (o.getKind() == SqlKind.DESCENDING || o.getKind() == SqlKind.NULLS_FIRST
              || o.getKind() == SqlKind.NULLS_LAST) {
            //is DESCENDING, nulls first, nulls last
            return ((SqlCall) o).getOperandList().get(0);
          }
          return o;
        })
        .map(o -> validator.expandOrderExpr(select, o))
        .collect(Collectors.toList());

    //If aggregating, replace each select item with ordinal
    if (validator.isAggregate(select)) {
      List<SqlNode> ordinals = getSelectListOrdinals(select, cleaned, mutableOrders.size());

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
      List<SqlNode> expanded = validator.getRawSelectScope(select).getExpandedSelectList();
      outer:
      for (SqlNode orderNode : cleaned) {
        //look for order in select list
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

  private SqlNode appendSubqueries(SqlNode from, SqlValidatorScope scope) {

    return null;
  }

  SqlNode rewriteFrom(SqlNode from, SqlValidatorScope scope) {
    //1. if SELF not in scope, add it (allow alias?)
    final SqlCall call;
//      final SqlNode[] operands;

    switch (from.getKind()) {
      case AS:
        call = (SqlCall) from;
        SqlNode firstOperand = call.operand(0);
        if (firstOperand instanceof SqlIdentifier) {
          from = convertTableName((SqlIdentifier) firstOperand,
              ((SqlIdentifier) call.getOperandList().get(1)).names.get(0), scope);
        } else {
          rewriteFrom(firstOperand, scope);
        }
      case TABLE_REF:
        break;
      case IDENTIFIER:
        from = rewriteTable((SqlIdentifier) from, scope);
        break;
      case JOIN:
        //from gets reassigned instead of replaced
        rewriteJoin((SqlJoin) from, scope);
        break;
      case SELECT:
        SqlValidatorScope subScope = validator.getFromScope((SqlSelect) from);
        rewriteQuery((SqlSelect) from, subScope);
        break;
      case INTERSECT:
      case EXCEPT:
      case UNION:
        break;
    }

    return from;
  }

  private SqlNode convertTableName(SqlIdentifier id, String alias, SqlValidatorScope scope) {
    final SqlValidatorNamespace fromNamespace =
        validator.getNamespace(id).resolve();

    if (fromNamespace.getNode() != null) {
      return rewriteFrom(fromNamespace.getNode(), scope);
    }

    if (fromNamespace instanceof ExpandableTableNamespace) {
      ExpandableTableNamespace tn = (ExpandableTableNamespace) fromNamespace;

      TablePath tablePath = tn.createTablePath(alias);

//        TablePathImpl tablePath = new TablePathImpl(baseTable, firstAlias, relative,
//        relationships, alias);
      JoinDeclaration declaration = JoinBuilder.expandPath(tablePath, false, joinBuilderFactory);
      declaration.getPullupCondition().ifPresent(pullupConditions::push);
      return declaration.getJoinTree();
    } else {
      //just do a simple mapping from table
      ScriptTable baseTable = fromNamespace.getTable().unwrap(ScriptTable.class);
      TableWithPK basePkTable = tableMapper.getTable(baseTable);
      return sqlNodeBuilder.createTableNode(basePkTable, Util.last(id.names));
    }

//      SqlQualified qualified = validator.getEmptyScope().fullyQualify(id);
////      SqlQualified qualified = scope.fullyQualify(id);
//      ScriptTable baseTable = fromNamespace.getTable().unwrap(ScriptTable.class);
//      Optional<String> baseAlias = Optional.ofNullable(qualified.prefix())
//          .filter(p -> p.size() == 1).map(p->p.get(0));
////      boolean isRelative = ns.getResolvedNamespace() instanceof RelativeTableNamespace;
//      List<Field> fields = baseTable.walkField(qualified.suffix());
//      List<Relationship> rels = fields.stream().filter(f->f instanceof Relationship).map( f->
//      (Relationship)f).collect(
//          Collectors.toList());

//      TableWithPK basePkTable = tableMapper.getTable(baseTable);
//      TablePathImpl tablePath = new TablePathImpl(baseTable, baseAlias, false, rels, alias);
//      JoinDeclaration declaration = expandPath(tablePath, false, joinBuilderFactory);
//
//
//
//      ScriptTable base = fromNamespace.getTable().unwrap(ScriptTable.class);
//
//      final String datasetName = null;
////          datasetStack.isEmpty() ? null : datasetStack.peek();
//      final boolean[] usedDataset = {false};
//      RelOptTable table =
//          SqlValidatorUtil.getRelOptTable(fromNamespace, catalogReader,
//              datasetName, usedDataset);

  }

  private SqlNode extraFromItems(SqlNode from, SqlValidatorScope scope) {
    List<InlineAggExtractResult> inlineAggs = inlineAgg.get(scope);
    for (InlineAggExtractResult agg : inlineAggs) {
      from = sqlNodeBuilder.createJoin(JoinType.LEFT, from, agg.getQuery(), agg.getCondition());
    }

    List<JoinDeclaration> joinDecs = toOne.get(scope);
    for (JoinDeclaration agg : joinDecs) {
      from = sqlNodeBuilder.createJoin(JoinType.LEFT, from, agg.getJoinTree(),
          agg.getPullupCondition().orElse(SqlLiteral.createBoolean(true, SqlParserPos.ZERO)));
    }

    return from;
  }

  private SqlNode rewriteTable(SqlIdentifier id, SqlValidatorScope scope) {
    //Expand
    validator.getNamespace(id).resolve();
    SqlValidatorNamespace ns = validator.getNamespace(id).resolve();
    ScriptTable table = ns.getTable().unwrap(ScriptTable.class);
    TableWithPK t = tableMapper.getTable(table);

    return sqlNodeBuilder.as(new SqlIdentifier(t.getNameId(), SqlParserPos.ZERO),
        Util.last(id.names));
  }

  private void rewriteJoin(SqlJoin join, SqlValidatorScope rootScope) {
    final SqlValidatorScope scope = validator.getJoinScope(join);
    SqlNode left = join.getLeft();
    SqlNode right = join.getRight();
    final SqlValidatorScope leftScope =
        Util.first(validator.getJoinScope(left),
            ((DelegatingScope) rootScope).getParent());

    final SqlValidatorScope rightScope =
        Util.first(validator.getJoinScope(right),
            ((DelegatingScope) rootScope).getParent());

    SqlNode l = rewriteFrom(left, leftScope);
    join.setLeft(l);
    SqlNode r = rewriteFrom(right, rightScope);
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
//        condition = convertNaturalCondition(validator.getNamespace(left),
//            validator.getNamespace(right));
//        rightRel = tempRightRel;
    } else {
      switch (conditionType) {
        case NONE:

          SqlNode newNoneCondition = condition.orElse(
              SqlLiteral.createBoolean(true, SqlParserPos.ZERO));
          join.setOperand(2, SqlLiteral.createSymbol(JoinType.INNER, SqlParserPos.ZERO));
          join.setOperand(4, SqlLiteral.createSymbol(JoinConditionType.ON, SqlParserPos.ZERO));
          join.setOperand(5, newNoneCondition);
          //TODO: Convert condition to ON w/ TRUE or an appended condition
//            condition = rexBuilder.makeLiteral(true);
//            rightRel = tempRightRel;
          break;
        case USING:
          //todo: Using
//            condition = convertUsingCondition(join,
//                validator.getNamespace(left),
//                validator.getNamespace(right));
//            rightRel = tempRightRel;
          break;
        case ON:
          SqlNode newOnCondition = condition
              .map(c -> and(join.getCondition(), c))
              .orElse(join.getCondition());
          join.setOperand(5, newOnCondition);
          //todo: append on
//            org.apache.calcite.util.Pair<RexNode, RelNode> conditionAndRightNode =
//            convertOnCondition(fromBlackboard,
//                join,
//                leftRel,
//                tempRightRel);
//            condition = conditionAndRightNode.left;
//            rightRel = conditionAndRightNode.right;
          break;
        default:
          throw Util.unexpected(conditionType);
      }
    }
  }

  class TranspileScope {

    SqlValidatorScope validatorScope;
    Optional<ScriptTable> context;
  }


}