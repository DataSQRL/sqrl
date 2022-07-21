package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.ImportedSqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.VirtualSqrlTable;
import lombok.Getter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

@Getter
public class DatasetTable extends VarTable {

  private final VirtualSqrlTable vtable;
  private final ImportedSqrlTable impTable;
  private DelegateVTable table;

  public DatasetTable(@NonNull NamePath path, ImportedSqrlTable impTable,
      VirtualSqrlTable vtable) {
    super(path);
    this.impTable = impTable;
    this.vtable = vtable;
    Name tableName = impTable.getSourceTableImport().getTable().getName();
    DelegateVTable dt = new DelegateVTable(tableName, vtable);
    this.table = dt;
    this.fields.addField(new RootTableField(dt));
  }

  public Map<Field, String> getFieldNameMap() {
    return new HashMap<>(table.getFieldNameMap());
  }

  public Map<VarTable, AbstractSqrlTable> getShredTableMap() {
    Map<VarTable, AbstractSqrlTable> map = new HashMap<>();
    map.put(table, vtable);
    map.putAll(table.getShredTableMap());
    return map;
  }

  public Map<String, AbstractSqrlTable> getShredTables() {
    Map<String, AbstractSqrlTable> map = new HashMap<>();
    map.put(vtable.getNameId(), vtable);
    map.putAll(table.getShredTables());
    return map;
  }
}
