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
  Map<DatasetOrTable, List<RelDataTypeField>> fields = new HashMap<>();

  /**
   * We can arrive at a table though multiple paths. We need to keep track
   * of the fields discovered on each unique logical table as well as the
   * path so we can rewrite it later.
   */
  public Table translate(DatasetOrTable table, String path) {
    fields.computeIfAbsent(table, e->new ArrayList<>());
    //TODO: Add all primary & fks
    return new CalciteSqrlTable(table, path, fields.get(table));
  }
}
