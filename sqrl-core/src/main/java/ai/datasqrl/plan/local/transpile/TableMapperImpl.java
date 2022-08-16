package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.schema.SQRLTable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class TableMapperImpl implements TableMapper {

  @Getter
  Map<SQRLTable, VirtualRelationalTable> tableMap;

  @Override
  public TableWithPK getTable(SQRLTable table) {
    return tableMap.get(table);
  }

  @Override
  public SQRLTable getScriptTable(VirtualRelationalTable vt) {
    for (Map.Entry<SQRLTable, VirtualRelationalTable> t : getTableMap()
        .entrySet()) {
      if (t.getValue().equals(vt)) {
        return t.getKey();
      }
    }
    return null;
  }
}