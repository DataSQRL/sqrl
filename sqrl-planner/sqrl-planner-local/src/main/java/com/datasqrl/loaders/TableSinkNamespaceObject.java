package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.tables.AbstractExternalTable;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.TableNamespaceObject;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class TableSinkNamespaceObject implements TableNamespaceObject<TableSink>, TableSinkObject {

  private final TableSink table;

  @Override
  public Name getName() {
    return table.getName();
  }

  @Override
  public TableSink getSink() {
    return table;
  }
}
