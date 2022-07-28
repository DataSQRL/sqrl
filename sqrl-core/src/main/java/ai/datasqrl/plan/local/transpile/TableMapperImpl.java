package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.sqrl.table.AbstractSqrlTable;
import ai.datasqrl.plan.calcite.sqrl.table.TableWithPK;
import ai.datasqrl.schema.ScriptTable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class TableMapperImpl implements TableMapper {

  @Getter
  Map<ScriptTable, AbstractSqrlTable> tableMap;

  @Override
  public TableWithPK getTable(ScriptTable table) {
    return tableMap.get(table);
  }
}