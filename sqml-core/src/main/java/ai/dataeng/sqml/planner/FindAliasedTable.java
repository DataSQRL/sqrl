package ai.dataeng.sqml.planner;

import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.sql.SqrlOperatorTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;

@AllArgsConstructor
public class FindAliasedTable extends SqlBasicVisitor {

  String alias;
  SqlValidator validator;

  private Optional<Table> table;

  public static Optional<Table> findTable(SqlNode sqlNode, String alias,
      SqlValidator validator) {
    FindAliasedTable findTable = new FindAliasedTable(alias, validator, Optional.empty());
    sqlNode.accept(findTable);
    return findTable.getTable();
  }

  @Override
  public Object visit(SqlCall call) {
    if (call.getOperator() == SqrlOperatorTable.AS) {
      SqlIdentifier alias = (SqlIdentifier)call.getOperandList().get(1);
      if (alias.names.get(0).equals(this.alias)) {
        SqlValidatorNamespace namespace = validator.getNamespace(call.getOperandList().get(0));
        if (namespace == null) {
          return null;
        }
        Object o = namespace
            .getRowType();
        if (o instanceof SqrlCalciteTable) {
          this.table = Optional.of(((SqrlCalciteTable) o).getSqrlTable());
        }
      }
    }
    return super.visit(call);
  }

  public Optional<Table> getTable() {
    return table;
  }
}
