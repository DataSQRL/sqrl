package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.calcite.table.TableWithPK;
import ai.datasqrl.schema.SQRLTable;

public interface TableMapper {
    TableWithPK getTable(SQRLTable table);
  }

