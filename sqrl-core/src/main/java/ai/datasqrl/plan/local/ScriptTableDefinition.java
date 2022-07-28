package ai.datasqrl.plan.local;

import ai.datasqrl.plan.calcite.table.QueryRelationalTable;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.SQRLTable;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

@Getter
public class ScriptTableDefinition {

  private final QueryRelationalTable baseTable;
  private final Map<SQRLTable, VirtualRelationalTable> shredTableMap;

  public ScriptTableDefinition(@NonNull QueryRelationalTable baseTable,
                               @NonNull Map<SQRLTable, VirtualRelationalTable> shredTableMap) {
    this.baseTable = baseTable;
    this.shredTableMap = shredTableMap;
  }

  public SQRLTable getTable() {
    return shredTableMap.entrySet().stream().filter(e -> e.getValue().isRoot()).map(Map.Entry::getKey).findFirst().get();
  }

  /**
   * Produces a mapping from SQRL Table fields to the corresponding
   * fields in Calcite.
   * Since import tables are generated from a single {@link ai.datasqrl.schema.builder.AbstractTableFactory.UniversalTableBuilder}
   * we can make the simplifying assumption that the names are identical.
   * @return
   */
  public Map<Field, String> getFieldNameMap() {
    Map<Field, String> fieldMap = new HashMap<>();
    shredTableMap.entrySet().stream().forEach( e -> {
      e.getKey().getColumns(false).forEach(f -> {
        String fieldName = f.getId().getCanonical();
        Preconditions.checkArgument(e.getValue().getRowType().getField(fieldName,true, false)!=null);
        fieldMap.put(f,fieldName);
      });
    });
    return fieldMap;
  }

  public Map<SQRLTable, VirtualRelationalTable> getShredTableMap() {
    return shredTableMap;
  }

}
