package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.TableNamespaceObject;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TableSourceSinkNamespaceObject implements TableNamespaceObject<TableSource>, TableSinkObject, TableSourceObject {

  private final TableSource table;
  private final TableSink sink;

  @Override
  public Name getName() {
    return table.getName();
  }

  @Override
  public TableSource getSource() {
    return table;
  }
}
