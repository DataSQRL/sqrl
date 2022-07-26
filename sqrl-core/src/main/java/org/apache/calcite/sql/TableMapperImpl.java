package org.apache.calcite.sql;

import ai.datasqrl.plan.calcite.sqrl.table.TableWithPK;
import ai.datasqrl.plan.calcite.sqrl.table.VirtualSqrlTable;
import ai.datasqrl.schema.ScriptTable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class TableMapperImpl implements TableMapper {

  @Getter
  Map<ScriptTable, VirtualSqrlTable> tableMap;

  @Override
  public TableWithPK getTable(ScriptTable table) {
    return tableMap.get(table);
  }
}