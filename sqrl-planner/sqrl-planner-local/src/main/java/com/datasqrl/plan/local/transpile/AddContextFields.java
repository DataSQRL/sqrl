/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.local.transpile;

import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.plan.local.generate.SqlNodeFactory;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.flink.util.Preconditions;

/**
 * Orders.entries.x := SELECT max(discount) AS bestDiscount FROM @; -> Orders.entries.x := SELECT
 * pk, max(discount) AS bestDiscount FROM @ GROUP BY pk;
 */
public class AddContextFields {

  private final SqlValidator sqrlValidator;
  private final Optional<VirtualRelationalTable> context;
  private final boolean aggregate;

  public AddContextFields(SqlValidator sqrlValidator,
      Optional<VirtualRelationalTable> context, boolean aggregate) {
    this.sqrlValidator = sqrlValidator;
    this.context = context;
    this.aggregate = aggregate;
  }

  public SqlNode accept(SqlNode node) {
    if (context.isEmpty()) {
      return node;
    }
    switch (node.getKind()) {
      case UNION:
        SqlCall call = (SqlCall) node;
        call.getOperandList()
            .forEach(this::accept);
        return call;
    }

    if (node instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) node;
      SqlNodeFactory factory = new SqlNodeFactory(select.getParserPosition());

      if (!select.isDistinct() &&
          !(context.isPresent()
              && select.getFetch() != null)
          || aggregate) { //we add this to the hint instead of these keywords
        rewriteGroup(select, factory);
        rewriteOrder(select, factory);
      }

      rewriteSelect(select, factory);
    }

    return node;
  }

  private void rewriteSelect(SqlSelect select, SqlNodeFactory factory) {
    List<SqlNode> ppkNodes = getPPKNodes(context, factory);
    List<SqlNode> toPrepend = ppkNodes.stream()
        .map(ppk -> factory.callAs(ppk, "__pk_" + ppkNodes.indexOf(ppk)))
        .collect(Collectors.toList());

    SqlNodeList prependList = factory.prependList(select.getSelectList(), toPrepend);
    select.setSelectList(prependList);
  }

  private void rewriteGroup(SqlSelect select, SqlNodeFactory factory) {
    if (!sqrlValidator.isAggregate(select)) {
      Preconditions.checkState(select.getGroup() == null);
      return;
    }

    List<SqlNode> ppkNodes = getPPKNodes(context, factory);

    SqlNodeList prependList = factory.prependList(select.getGroup(), ppkNodes);
    select.setGroupBy(prependList);
  }

  private void rewriteOrder(SqlSelect select, SqlNodeFactory factory) {
    //If no orders, exit
    if (select.getOrderList() == null || select.getOrderList().getList().isEmpty()) {
      return;
    }

    List<SqlNode> ppkNodes = getPPKNodes(context, factory);
    SqlNodeList prependList = factory.prependList(select.getOrderList(), ppkNodes);
    select.setOrderBy(prependList);
  }

  public static List<SqlNode> getPPKNodes(Optional<VirtualRelationalTable> context,
      SqlNodeFactory factory) {
    return context.get().getPrimaryKeyNames()
        .stream()
        .map(ppk -> factory.toIdentifier(ReservedName.SELF_IDENTIFIER.getCanonical(), ppk))
        .collect(Collectors.toList());
  }
}
