package com.datasqrl.loaders;

import java.util.Optional;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
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
