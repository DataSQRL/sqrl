package com.datasqrl.plan.local.generate;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.plan.table.CalciteTableFactory;
import lombok.Getter;

import java.util.Optional;
import org.apache.calcite.sql.SqrlTableFunctionDef;

@Getter
public class SqrlTableNamespaceObject extends AbstractTableNamespaceObject<ScriptTableDefinition> {
  private final Name name;
  private final ScriptTableDefinition table;
  private final SqrlTableFunctionDef args;

  public SqrlTableNamespaceObject(Name name, ScriptTableDefinition table, CalciteTableFactory tableFactory,
      SqrlTableFunctionDef args) {
    super(tableFactory, Optional.of(args));
    this.name = name;
    this.table = table;
    this.args = args;
  }

  @Override
  public boolean apply(Optional<String> objectName, SqrlFramework framework, ErrorCollector errors) {
    registerScriptTable(table, framework);
    return true;
  }
}
