package org.apache.calcite.sql.validate;

import com.google.common.base.Preconditions;
import java.util.Optional;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.schema.SqrlSchema;
import org.apache.calcite.schema.Table;

public class TableAliasScope {

  private final SqrlSchema schema;
  private final ListScope joinScope;

  public TableAliasScope(SqrlSchema schema, ListScope scope) {
    this.schema = schema;
    this.joinScope = scope;
  }

  public Optional<Table> getOrMaybeCreateTable(String name) {
    if (!name.contains(".")) return Optional.empty();
    String prefix = name.substring(0, name.indexOf("."));
    String tableName = name.substring(name.indexOf(".") + 1);
    for (ScopeChild scope : joinScope.children) {
      if (scope.name.equalsIgnoreCase(prefix)) {
        Preconditions.checkState(scope.namespace instanceof IdentifierNamespace);
        Preconditions.checkState(scope.namespace.getRowType() instanceof SqrlCalciteTable);
        SqrlCalciteTable table = (SqrlCalciteTable)scope.namespace.getRowType();

        return schema.getTableFactory().createPath(table.getSqrlTable(), tableName);
      }
    }
    return Optional.empty();
  }
}
