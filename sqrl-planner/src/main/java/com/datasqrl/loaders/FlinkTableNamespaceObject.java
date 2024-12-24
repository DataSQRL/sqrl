package com.datasqrl.loaders;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.plan.local.generate.AbstractTableNamespaceObject;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.validate.ScriptPlanner;
import java.nio.file.Path;
import java.util.Optional;
import lombok.Getter;
import lombok.Value;

@Getter
public class FlinkTableNamespaceObject implements TableNamespaceObject<FlinkTable> {

  private final FlinkTable table;

  public FlinkTableNamespaceObject(FlinkTable table) {
    this.table = table;
  }

  @Override
  public Name getName() {
    return table.getName();
  }

  @Override
  public boolean apply(ScriptPlanner planner, Optional<String> alias, SqrlFramework framework,
      ErrorCollector errors) {
    throw new UnsupportedOperationException();
  }


  @Value
  public static class FlinkTable {
    Name name;
    String flinkSQL;
    Path flinkSqlFile;
    Optional<TableSchema> schema;
  }

}
