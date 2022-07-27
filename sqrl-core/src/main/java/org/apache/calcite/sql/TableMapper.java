package org.apache.calcite.sql;

import ai.datasqrl.plan.calcite.sqrl.table.TableWithPK;
import ai.datasqrl.schema.ScriptTable;

public interface TableMapper {
    TableWithPK getTable(ScriptTable table);
  }

