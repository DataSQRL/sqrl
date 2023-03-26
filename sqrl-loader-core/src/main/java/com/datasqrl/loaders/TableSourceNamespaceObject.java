package com.datasqrl.loaders;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.plan.local.generate.TableNamespaceObject;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TableSourceNamespaceObject implements TableNamespaceObject<TableSource> {

  private final TableSource table;

  @Override
  public Name getName() {
    return table.getName();
  }
}
