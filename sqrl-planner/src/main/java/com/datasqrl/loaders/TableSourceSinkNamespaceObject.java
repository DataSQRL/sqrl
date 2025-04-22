package com.datasqrl.loaders;

import java.util.Optional;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.plan.local.generate.AbstractTableNamespaceObject;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.validate.ScriptPlanner;

import lombok.Getter;

@Getter
public class TableSourceSinkNamespaceObject extends AbstractTableNamespaceObject<TableSource> implements TableSinkObject, TableSourceObject {

  private final TableSource table;
  private final TableSink sink;

  public TableSourceSinkNamespaceObject(TableSource source, TableSink sink, CalciteTableFactory tableFactory,
      ModuleLoader moduleLoader) {
    super(tableFactory, NameCanonicalizer.SYSTEM, moduleLoader);
    this.table = source;
    this.sink = sink;
  }

  @Override
  public Name getName() {
    return table.getName();
  }

  @Override
  public boolean apply(ScriptPlanner planner, Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    return importSourceTable(objectName, table, framework, errors);
  }

  @Override
  public TableSource getSource() {
    return table;
  }
}
