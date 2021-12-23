package org.apache.calcite.jdbc;

import ai.dataeng.sqml.logical4.LogicalPlan.DatasetOrTable;
import org.apache.calcite.rel.type.OrdersSchema;
import org.apache.calcite.rel.type.OrdersSchema.SqrlTable;
import org.apache.calcite.schema.Table;

/**
 * Accepts a sqrl schema and translates it to a calcite schema
 */
public class SqrlToCalciteTableTranslator {

  SqrlTable table = new OrdersSchema.SqrlTable();
  public Table translate(DatasetOrTable table) {
    //All physical columns + special columns for relationships


//    SqrlTable orders =  new OrdersSchema.SqrlTable(List.of());




    return this.table;
  }
}
