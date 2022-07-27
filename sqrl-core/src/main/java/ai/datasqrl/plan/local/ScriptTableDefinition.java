package ai.datasqrl.plan.local;

import ai.datasqrl.plan.calcite.sqrl.table.QuerySqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.VirtualSqrlTable;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.ScriptTable;
import com.google.common.base.Preconditions;
import lombok.Getter;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;

@Getter
public class ScriptTableDefinition {

  private final QuerySqrlTable baseTable;
  private final Map<ScriptTable, VirtualSqrlTable> shredTableMap;

  public ScriptTableDefinition(@NonNull QuerySqrlTable baseTable,
                               @NonNull Map<ScriptTable, VirtualSqrlTable> shredTableMap) {
    this.baseTable = baseTable;
    this.shredTableMap = shredTableMap;
  }

  public ScriptTable getTable() {
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

  public Map<ScriptTable, VirtualSqrlTable> getShredTableMap() {
    return shredTableMap;
  }

}
