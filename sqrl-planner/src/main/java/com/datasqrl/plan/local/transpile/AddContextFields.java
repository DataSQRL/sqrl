package com.datasqrl.plan.local.transpile;

import com.datasqrl.plan.calcite.table.VirtualRelationalTable;
import com.datasqrl.plan.calcite.util.CalciteUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.flink.util.Preconditions;

/**
 *
 * Orders.entries.x := SELECT max(discount) AS bestDiscount
 *                     FROM _;
 * ->
 * Orders.entries.x := SELECT pk, max(discount) AS bestDiscount
 *                     FROM _
 *                     GROUP BY pk;
 *
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
      if (!select.isDistinct() &&
          !(context.isPresent()
              && select.getFetch() != null) || aggregate) { //we add this to the hint instead of these keywords
        rewriteGroup(select);
        rewriteOrder(select);
      }

      rewriteSelect(select);
    }

    return node;
  }

  private void rewriteSelect(SqlSelect select) {
    //add columns to select list. If a key already exists with that name, make unique
    List<SqlNode> identifiers = new ArrayList<>();
    List<SqlNode> ppkNodes = getPPKNodes(context);
    for (int i = 0; i < ppkNodes.size(); i++) {
      SqlNode ppk = ppkNodes.get(i);
      String alias = SqlValidatorUtil.getAlias(ppk, -1);

      identifiers.add(
          SqlStdOperatorTable.AS.createCall(
              SqlParserPos.ZERO,
              ppk,
              new SqlIdentifier(List.of("__pk_" + i), SqlParserPos.ZERO)
          ));
    }


    CalciteUtil.prependSelectListNodes(select, identifiers);
  }

  private void rewriteGroup(SqlSelect select) {
    if (!sqrlValidator.isAggregate(select)) {
      Preconditions.checkState(select.getGroup() == null);
      return;
    }

    List<SqlNode> ppkNodes = getPPKNodes(context);
    CalciteUtil.prependGroupByNodes(select, ppkNodes);
  }

  private void rewriteOrder(SqlSelect select) {
    //If no orders, exit
    if (select.getOrderList() == null || select.getOrderList().getList().isEmpty()) {
      return;
    }

    List<SqlNode> ppkNodes = getPPKNodes(context);
    CalciteUtil.prependOrderByNodes(select, ppkNodes);
  }

  public static List<SqlNode> getPPKNodes(Optional<VirtualRelationalTable> context) {
    List<SqlNode> identifiers = new ArrayList<>();
    for (String ppk : context.get().getPrimaryKeyNames()) {
      identifiers.add(
          new SqlIdentifier(List.of("_", ppk), SqlParserPos.ZERO)
      );
    }

    return identifiers;
  }
}
