package org.apache.calcite.jdbc;

import ai.dataeng.sqml.logical4.LogicalPlan.DatasetOrTable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.CalciteSqrlTable;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;

/**
 * Accepts a sqrl schema and translates it to a calcite schema.
 */
public class SqrlToCalciteTableTranslator {
//  Map<DatasetOrTable, CalciteSqrlTable> tables = new HashMap<>();
  Map<DatasetOrTable, List<RelDataTypeField>> fields = new HashMap<>();

  public Table translate(DatasetOrTable table, String path) {
    //If we can get to another table though path traversal, we want to know that.
//    if (tables.containsKey(table)) {
//      return tables.get(table);
//    }
    fields.computeIfAbsent(table, e->new ArrayList<>());
    //TODO: Add all primary & fks
    return new CalciteSqrlTable(table, path, fields.get(table));
//    return tables.get(table);
  }
}
