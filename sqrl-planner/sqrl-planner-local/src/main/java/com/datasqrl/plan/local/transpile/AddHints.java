/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.transpile;

import com.datasqrl.plan.calcite.hints.TopNHint;
import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.util.CalciteUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
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

  public void accept(boolean isDistinct, SqlNode node) {
    switch (node.getKind()) {
      case UNION:
        SqlCall call = (SqlCall) node;
        call.getOperandList()
            .forEach(o -> accept(isDistinct, o));
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
    } else if (isNested() && select.getFetch() != null) {
      rewriteDistinctingHint(select, (ppk) ->
          TopNHint.createSqlHint(TopNHint.Type.TOP_N, ppk, SqlParserPos.ZERO));
    } else if (isDistinct) {
      rewriteDistinctOnHint(select);
    }
  }
  private void rewriteDistinctOnHint(SqlSelect select) {
    SqlValidatorScope scope = validator.getSelectScope(select);

    //Rearrange distinct on to be the first few columns
    SqlHint hint = getDistinctOnHint(select);
    List<SqlNode> operands = extractHintOperands(hint);
    List<SqlNode> newSelectList = rearrangeDistinctOnFirstFewColumns(operands);

    List<SqlNode> withoutDuplicates = removeDuplicates(newSelectList, select, scope);
    List<SqlNode> finalSelectList = mergeLists(newSelectList, withoutDuplicates);
    select.setSelectList(new SqlNodeList(finalSelectList, SqlParserPos.ZERO));
    CalciteUtil.wrapSelectInProject(select);
    SqlSelect inner = (SqlSelect) select.getFrom();
    //pull up hints
    select.setHints(inner.getHints());
    inner.setHints(new SqlNodeList(SqlParserPos.ZERO));
  }

  /**
   * Extracts the operands from the distinct on hint.
   *
   * @param hint the distinct on hint
   * @return the operands from the hint
   */
  private List<SqlNode> extractHintOperands(SqlHint hint) {
    //Hint operand lists include hint name so skip first
    return ((SqlNodeList) hint.getOperandList().get(1)).getList();
  }

  /**
   * Rearranges the distinct on columns to be the first few columns.
   *
   * @param operands the distinct on columns
   * @return the rearranged select list
   */
  private List<SqlNode> rearrangeDistinctOnFirstFewColumns(List<SqlNode> operands) {
    List<SqlNode> newSelectList = new ArrayList<>();
    for (int i = 0; i < operands.size(); i++) {
      SqlNode operand = operands.get(i);
      newSelectList.add(operand);
      operands.set(i, new SqlIdentifier(Integer.toString(i), SqlParserPos.ZERO));
    }
    return newSelectList;
  }

  /**
   * Merges two lists into one.
   *
   * @param newSelectList the new select list
   * @param withoutDuplicates the list without duplicates
   * @return the merged list
   */
  private List<SqlNode> mergeLists(List<SqlNode> newSelectList, List<SqlNode> withoutDuplicates) {
    List<SqlNode> finalSelectList = new ArrayList<>(newSelectList);
    finalSelectList.addAll(withoutDuplicates);
    return finalSelectList;
  }

  private List<SqlNode> removeDuplicates(List<SqlNode> newSelectList, SqlSelect select,
      SqlValidatorScope scope) {
    SelectScope selectScope = validator.getRawSelectScope(select);
    List<SqlNode> items = selectScope.getExpandedSelectList();
    List<SqlIdentifier> fullyQualifiedIdentifiers = fullyQualifyIdentifiers(newSelectList, scope);

    return filterDuplicateItems(items, fullyQualifiedIdentifiers, scope);
  }

  private List<SqlNode> filterDuplicateItems(List<SqlNode> items,
      List<SqlIdentifier> fullyQualifiedIdentifiers, SqlValidatorScope scope) {
    List<SqlNode> newNodes = new ArrayList<>();
    for (SqlNode item : items) {
      if (item instanceof SqlCall) {
        SqlCall call = (SqlCall) item;
        SqlIdentifier alias = (SqlIdentifier) call.getOperandList().get(1);
        Optional<SqlIdentifier> found = fullyQualifiedIdentifiers.stream()
            .filter(i -> fullyQualifyIdentifier(alias, scope).equalsDeep(i, Litmus.IGNORE))
            .findAny();
        if (found.isEmpty()) {
          newNodes.add(item);
        }
      } else if (item instanceof SqlIdentifier) {
        Optional<SqlIdentifier> found = fullyQualifiedIdentifiers.stream()
            .filter(i -> fullyQualifyIdentifier(item, scope).equalsDeep(i, Litmus.IGNORE))
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

  private SqlIdentifier fullyQualifyIdentifier(SqlNode item, SqlValidatorScope scope) {
    return scope.fullyQualify((SqlIdentifier) item).identifier;
  }

  private List<SqlIdentifier> fullyQualifyIdentifiers(List<SqlNode> newSelectList,
      SqlValidatorScope scope) {
    return newSelectList.stream()
        .filter(f -> f instanceof SqlIdentifier)
        .map(i -> fullyQualifyIdentifier(i, scope))
        .collect(Collectors.toList());
  }

  private SqlHint getDistinctOnHint(SqlSelect select) {
    return select.getHints().getList().stream()
        .filter(hint -> ((SqlHint) hint).getName().equals(TopNHint.Type.DISTINCT_ON.name()))
        .findFirst()
        .map(hint -> (SqlHint) hint)
        .orElseThrow(() -> new RuntimeException("Missing hint"));
  }

  private boolean isNested() {
    return context.isPresent();
  }


  private void rewriteDistinctingHint(SqlSelect select, Function<SqlNodeList, SqlHint> createFnc) {
    CalciteUtil.removeKeywords(select);

    CalciteUtil.wrapSelectInProject(select);

    List<SqlNode> innerPPKNodes = context.map(c -> getPPKNodes()).orElse(List.of());
    SqlNodeList ppkNode = new SqlNodeList(innerPPKNodes, SqlParserPos.ZERO);
    SqlHint selectDistinctHint = createFnc.apply(ppkNode);
    CalciteUtil.setHint(select, selectDistinctHint);
  }

  private List<SqlNode> getPPKNodes() {
    return IntStream.range(0, context.get().getPrimaryKeyNames().size())
        .mapToObj(i -> new SqlIdentifier(List.of(Integer.toString(i)), SqlParserPos.ZERO))
        .collect(Collectors.toList());
  }
}
