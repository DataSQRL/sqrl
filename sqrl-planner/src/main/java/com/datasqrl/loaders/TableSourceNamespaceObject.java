package com.datasqrl.loaders;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.local.generate.AbstractTableNamespaceObject;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.validate.ScriptPlanner;
import lombok.Getter;

import java.util.Optional;

@Getter
public class TableSourceNamespaceObject extends AbstractTableNamespaceObject<TableSource> implements TableSourceObject {

  private final TableSource table;

  public TableSourceNamespaceObject(TableSource table, CalciteTableFactory tableFactory,
      ModuleLoader moduleLoader) {
    super(tableFactory, NameCanonicalizer.SYSTEM, moduleLoader);
    this.table = table;
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
