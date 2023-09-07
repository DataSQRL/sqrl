package com.datasqrl.loaders;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.local.generate.AbstractTableNamespaceObject;
import com.datasqrl.plan.table.CalciteTableFactory;
import lombok.Getter;

import java.util.Optional;

@Getter
public class TableSourceSinkNamespaceObject extends AbstractTableNamespaceObject<TableSource> implements TableSinkObject, TableSourceObject {

  private final TableSource table;
  private final TableSink sink;

  public TableSourceSinkNamespaceObject(TableSource source, TableSink sink, CalciteTableFactory tableFactory) {
    super(tableFactory, Optional.empty());
    this.table = source;
    this.sink = sink;
  }

  @Override
  public Name getName() {
    return table.getName();
  }

  @Override
  public boolean apply(Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    return importSourceTable(objectName, table, framework);
  }

  @Override
  public TableSource getSource() {
    return table;
  }
}
