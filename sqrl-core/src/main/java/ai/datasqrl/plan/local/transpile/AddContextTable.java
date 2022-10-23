package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.parse.tree.name.ReservedName;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Orders.x := SELECT * FROM Product;
 * ->
 * Orders.x := SELECT * FROM orders$1 AS _ JOIN Product;
 *
 * Does not add self table if one is already present and is properly aliased.
 * Also adds self to expressions.
 */
@AllArgsConstructor
public class AddContextTable extends SqlShuttle {
  String selfVirtualTableName;

  public SqlNode accept(SqlNode query) {
    if (query instanceof SqlSelect) {
      SqlSelect select = (SqlSelect) query;
      if (select.getFrom() == null) {
        select.setFrom(SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
            new SqlIdentifier(selfVirtualTableName, SqlParserPos.ZERO),
            new SqlIdentifier(ReservedName.SELF_IDENTIFIER.getCanonical(), SqlParserPos.ZERO)));
      } else {
        select.setFrom(select.getFrom().accept(this));
      }
    }
    return query;
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    //Already exists
    if (id.names.size() == 1 && id.names.get(0).equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical())) {
      return createConcreteSelfTable();
    }
    return addSelfLeftDeep(id);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    ValidateNoReservedAliases noReservedAliases = new ValidateNoReservedAliases();
    call.accept(noReservedAliases);

    switch (call.getKind()) {
      case AS:
      case SELECT:
        // (subquery)
        SqlNode newCall = addSelfLeftDeep(call);
        return newCall;
      case JOIN:
        //walk left deep
        SqlJoin join =(SqlJoin) call;
        SqlNode node = join.getLeft();
        SqlNode newLeft = node.accept(this);
        join.setLeft(newLeft);
        return join;
    }
    return super.visit(call);
  }

  private SqlNode addSelfLeftDeep(SqlNode node) {
    return new SqlJoin(
        SqlParserPos.ZERO,
        createConcreteSelfTable(),
        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
        JoinType.DEFAULT.symbol(SqlParserPos.ZERO),
        node,
        JoinConditionType.NONE.symbol(SqlParserPos.ZERO),
        null
    );
  }

  private SqlNode createConcreteSelfTable() {
    return SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO,
        new SqlIdentifier(selfVirtualTableName, SqlParserPos.ZERO),
        new SqlIdentifier(ReservedName.SELF_IDENTIFIER.getCanonical(), SqlParserPos.ZERO));
  }

  class ValidateNoReservedAliases extends SqlBasicVisitor<SqlNode> {
    @Override
    public SqlNode visit(SqlCall call) {
      switch (call.getKind()) {
        case AS:
          validateNotReserved((SqlIdentifier)call.getOperandList().get(1));
      }
      return super.visit(call);
    }

    private void validateNotReserved(SqlIdentifier identifier) {
      Preconditions.checkState(identifier.names.size() == 1 &&
              !identifier.names.get(0).equalsIgnoreCase(ReservedName.SELF_IDENTIFIER.getCanonical()),
          "The SELF keyword is reserved, use a different alias"
      );
    }
  }
}
