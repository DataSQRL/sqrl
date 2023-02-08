package com.datasqrl.loaders;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.plan.local.generate.TableNamespaceObject;
import org.apache.calcite.schema.Table;

public class TableSourceNamespaceObject implements TableNamespaceObject<TableSource> {

  private final TableSource table;

  public TableSourceNamespaceObject(TableSource table) {
    this.table = table;
  }

  @Override
  public Name getName() {
    return table.getName();
  }

  @Override
  public TableSource getTable() {
    return table;
  }
}
