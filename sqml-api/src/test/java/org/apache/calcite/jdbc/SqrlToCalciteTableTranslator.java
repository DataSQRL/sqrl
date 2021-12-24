package org.apache.calcite.jdbc;

import ai.dataeng.sqml.logical4.LogicalPlan.DatasetOrTable;
import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rel.type.CalciteSqrlTable;
import org.apache.calcite.schema.Table;

/**
 * Accepts a sqrl schema and translates it to a calcite schema
 */
public class SqrlToCalciteTableTranslator {

  Map<DatasetOrTable, CalciteSqrlTable> tables = new HashMap<>();
  public Table translate(DatasetOrTable table) {
    //All physical columns + special columns for relationships
    if (tables.containsKey(table)) {
      return tables.get(table);
    }
    tables.put(table, new CalciteSqrlTable(table));
    return tables.get(table);

  }
}
