package com.datasqrl.loaders;

import java.nio.file.Path;
import java.util.Optional;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.module.TableNamespaceObject;

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

  @Value
  public static class FlinkTable {
    Name name;
    String flinkSQL;
    Path flinkSqlFile;
    Optional<TableSchema> schema;
  }

}
