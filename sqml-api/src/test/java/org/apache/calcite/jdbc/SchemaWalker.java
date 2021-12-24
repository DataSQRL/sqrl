package org.apache.calcite.jdbc;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.LogicalPlan.DatasetOrTable;
import ai.dataeng.sqml.tree.name.NamePath;
import lombok.AllArgsConstructor;
import org.apache.calcite.schema.Table;

/**
 * Walks a sqrl schema to resolve a calcite table.
 */
@AllArgsConstructor
public class SchemaWalker implements TableResolver {
  NamePath context;
  LogicalPlan logicalPlan;
  SqrlToCalciteTableTranslator tableTranslator;

  @Override
  public Table resolve(String path) {
    NamePath namePath = NamePath.parse(path);
//    if (namePath.isEmpty()) {
//      return null;
//    }
//
//    NamePath resolved;
//    if (namePath.get(0).getCanonical().equalsIgnoreCase("@")) {
//      resolved = context.resolve(namePath.popFirst());
//    } else {
//      resolved = namePath;
//    }

    DatasetOrTable table = logicalPlan.getSchema().walk(namePath);
    Table table2 = tableTranslator.translate(table, path);

    return table2;
  }
}
