package com.datasqrl.plan.local.transpile;

import com.datasqrl.plan.calcite.hints.TopNHint;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.calcite.util.CalciteUtil;
import com.datasqrl.plan.local.generate.Resolve.StatementKind;
import com.datasqrl.plan.local.generate.Resolve.StatementOp;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;

/**
 * Adds hints for nested limits, distinct on, or select distinct
 */
public class AddHints {

  private final SqlValidator validator;
  private final Optional<VirtualRelationalTable> context;

  public AddHints(SqlValidator validator, Optional<VirtualRelationalTable> context) {
    this.validator = validator;
    this.context = context;
  }

  public void accept(StatementOp op, SqlNode node) {
    switch (node.getKind()) {
      case UNION:
        SqlCall call = (SqlCall) node;
        call.getOperandList()
            .forEach(o -> accept(op, o));
        return;
    }

    if (!(node instanceof SqlSelect)) {
      return;
    }

    SqlSelect select = (SqlSelect) node;
    if (select.isDistinct() && select.getGroup() != null) {
      throw new RuntimeException("SELECT DISTINCT with a GROUP not yet supported");
    }

    if (select.isDistinct()) {
      rewriteDistinctingHint(select, (ppk) ->
          TopNHint.createSqlHint(TopNHint.Type.SELECT_DISTINCT, ppk, SqlParserPos.ZERO));
    } else if (isNested(op) && select.getFetch() != null) {
      rewriteDistinctingHint(select, (ppk) ->
          TopNHint.createSqlHint(TopNHint.Type.TOP_N, ppk, SqlParserPos.ZERO));
    } else if (op.getStatementKind() == StatementKind.DISTINCT_ON) {
      rewriteDistinctOnHint(select);
    }
  }

  private void rewriteDistinctOnHint(SqlSelect select) {
    SqlValidatorScope scope = validator.getSelectScope(select);

    //1. Rearrange so distinct on is first few columns
    SqlHint hint = getDistinctOnHint(select);

    SelectScope selectScope = validator.getRawSelectScope(select);
    List<SqlNode> items = selectScope.getExpandedSelectList();

    List<SqlNode> newSelectList = new ArrayList<>();
    //Hint operand lists include hint name so skip first
    List<SqlNode> operands = ((SqlNodeList) hint.getOperandList().get(1)).getList();
    for (int i = 0; i < operands.size(); i++) {
      SqlNode operand = operands.get(i);
      newSelectList.add(operand);
      operands.set(i, new SqlIdentifier(Integer.toString(i), SqlParserPos.ZERO));
    }

    List<SqlNode> list = removeDuplicates(newSelectList, items, scope);
    List<SqlNode> newList = new ArrayList<>(newSelectList);
    newList.addAll(list);

    select.setSelectList(new SqlNodeList(newList, SqlParserPos.ZERO));
    CalciteUtil.wrapSelectInProject(select);
    SqlSelect inner = (SqlSelect) select.getFrom();
    //pull up hints
    select.setHints(inner.getHints());
    inner.setHints(new SqlNodeList(SqlParserPos.ZERO));
  }

  private List<SqlNode> removeDuplicates(List<SqlNode> newSelectList, List<SqlNode> items,
      SqlValidatorScope scope) {
    List<SqlIdentifier> identifiers = newSelectList.stream().filter(f -> f instanceof SqlIdentifier)
        .map(i -> scope.fullyQualify((SqlIdentifier) i).identifier)
        .collect(Collectors.toList());

    List<SqlNode> newNodes = new ArrayList<>();
    for (SqlNode item : items) {
      if (item instanceof SqlIdentifier) {
        Optional<SqlIdentifier> found = identifiers.stream().filter(i ->
                scope.fullyQualify((SqlIdentifier) item).identifier.equalsDeep(i, Litmus.IGNORE))
            .findAny();
        if (found.isEmpty()) {
          newNodes.add(item);
        }
      } else {
        newNodes.add(item);
      }
    }

    return newNodes;
  }

  private List<SqlNode> skipFirst(List<SqlNode> operandList) {
    return operandList.subList(1, operandList.size());
  }

  private SqlHint getDistinctOnHint(SqlSelect select) {
    List<SqlNode> list = select.getHints().getList();
    for (int i = 0; i < list.size(); i++) {
      SqlHint hint = (SqlHint) list.get(i);
      if (hint.getName().equals(TopNHint.Type.DISTINCT_ON.name())) {
        return hint;
      }
    }
    throw new RuntimeException("Missing hint");
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
        SqlHint newHint = rewriteDistinctOnHint(select, hint, scope);
        hints.set(i, newHint);
      }
    }

    select.setHints(hints);
  }

  private void rewriteDistinctingHint(SqlSelect select, Function<SqlNodeList, SqlHint> createFnc) {
    CalciteUtil.removeKeywords(select);

    CalciteUtil.wrapSelectInProject(select);

    List<SqlNode> innerPPKNodes = context.map(c -> getPPKNodes(select)).orElse(List.of());
    SqlNodeList ppkNode = new SqlNodeList(innerPPKNodes, SqlParserPos.ZERO);
    SqlHint selectDistinctHint = createFnc.apply(ppkNode);
    CalciteUtil.setHint(select, selectDistinctHint);
  }

  private List<SqlNode> getPPKNodes(SqlSelect select) {
    return IntStream.range(0, context.get().getPrimaryKeyNames().size())
        .mapToObj(i -> new SqlIdentifier(List.of(Integer.toString(i)), SqlParserPos.ZERO))
        .collect(Collectors.toList());
  }

  private SqlHint rewriteDistinctOnHint(SqlSelect select, SqlHint hint, SqlValidatorScope scope) {
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
}
