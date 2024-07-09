package com.datasqrl.loaders;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.plan.validate.ScriptPlanner;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;

@AllArgsConstructor
@Getter
public class TableSinkNamespaceObject implements TableNamespaceObject<TableSink>, TableSinkObject {

  private final TableSink table;

  @Override
  public Name getName() {
    return table.getName();
  }

  @Override
  public boolean apply(ScriptPlanner planner, Optional<String> name, SqrlFramework framework, ErrorCollector errors) {
    throw new RuntimeException("Cannot import table sink");
  }

  @Override
  public TableSink getSink() {
    return table;
  }
}
