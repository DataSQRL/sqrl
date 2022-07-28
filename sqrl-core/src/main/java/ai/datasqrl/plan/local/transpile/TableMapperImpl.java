package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.AbstractRelationalTable;
import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.schema.SQRLTable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
public class TableMapperImpl implements TableMapper {

  @Getter
  Map<SQRLTable, AbstractRelationalTable> tableMap;

  @Override
  public TableWithPK getTable(SQRLTable table) {
    return tableMap.get(table);
  }
}