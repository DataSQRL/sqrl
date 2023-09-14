package com.datasqrl.plan.local.generate;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.module.TableNamespaceObject;
import com.datasqrl.plan.local.ScriptTableDefinition;
import com.datasqrl.plan.table.AbstractRelationalTable;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.schema.SQRLTable;
import java.util.Map;
import java.util.Optional;

public abstract class AbstractTableNamespaceObject<T> implements TableNamespaceObject<T> {

  private final CalciteTableFactory tableFactory;

  public AbstractTableNamespaceObject(CalciteTableFactory tableFactory) {
    this.tableFactory = tableFactory;
  }

  protected boolean importSourceTable(Optional<String> objectName, TableSource table, SqrlFramework framework) {
    ScriptTableDefinition scriptTableDefinition = tableFactory.importTable(table,
        objectName.map(n->tableFactory.getCanonicalizer().name(n)));

    registerScriptTable(scriptTableDefinition, framework);

    scriptTableDefinition.getShredTableMap().entrySet().stream()
        .filter(f->f.getKey() instanceof RootSqrlTable)
        .forEach(f->{
          framework.getSchema().addSqrlTable(f.getKey());
          framework.getSchema().plus().add(f.getKey().getPath().toString() + "$" + framework.getUniqueMacroInt().incrementAndGet(),
              (RootSqrlTable)f.getKey());
        });

    return true;
  }

  public void registerScriptTable(ScriptTableDefinition tblDef, SqrlFramework framework) {
    framework.getSchema()
        .add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());
    //add to schema
    for (Map.Entry<SQRLTable, VirtualRelationalTable> entry : tblDef.getShredTableMap().entrySet()) {
      framework.getSchema().add(entry.getValue().getNameId(), entry.getValue());
      entry.getValue().setSqrlTable(entry.getKey());
      entry.getKey().setVtTable(entry.getValue());

      framework.getSchema().addSqrlTable(entry.getKey());

      for (Field field : entry.getKey().getFields().getFields()) {
        if (field instanceof Relationship) {
          Relationship rel = (Relationship)field;
          framework.getSchema().plus().add(rel.getPath().toString() + "$" + framework.getUniqueMacroInt().incrementAndGet(),
              rel);
          framework.getSchema().getRelationships().put(rel.getPath().toStringList(),
              rel.getToTable().getPath().toStringList());
        }
      }
    }

    if (tblDef.getBaseTable() instanceof ProxyImportRelationalTable) {
      AbstractRelationalTable impTable = ((ProxyImportRelationalTable) tblDef.getBaseTable()).getBaseTable();
      framework.getSchema().add(impTable.getNameId(), impTable);
    }
  }
}
