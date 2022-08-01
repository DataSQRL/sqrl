package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.AbstractRelationalTable;
import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.schema.SQRLTable;
import java.util.Map;

public interface TableMapper {

  Map<SQRLTable, AbstractRelationalTable> getTableMap();

  TableWithPK getTable(SQRLTable table);

  SQRLTable getScriptTable(VirtualRelationalTable vt);
}

