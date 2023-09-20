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

    return true;
  }

  public void registerScriptTable(ScriptTableDefinition tblDef, SqrlFramework framework) {
    framework.getSchema()
        .add(tblDef.getBaseTable().getNameId(), tblDef.getBaseTable());
    //add to schema
    for (Map.Entry<SQRLTable, VirtualRelationalTable> entry : tblDef.getShredTableMap().entrySet()) {
      framework.getSchema().add(entry.getValue().getNameId(), entry.getValue());

      for (Field field : entry.getKey().getFields().getFields()) {
        //todo: this is only required because we miss registering nested tables for distinct-on statements
        // Add the logic to calcite table factory and remove this
        if (field instanceof Relationship) {
          framework.getSchema().addRelationship((Relationship) field);
        }
      }
    }

    if (tblDef.getBaseTable() instanceof ProxyImportRelationalTable) {
      AbstractRelationalTable impTable = ((ProxyImportRelationalTable) tblDef.getBaseTable()).getBaseTable();
      framework.getSchema().add(impTable.getNameId(), impTable);
    }
  }
}
