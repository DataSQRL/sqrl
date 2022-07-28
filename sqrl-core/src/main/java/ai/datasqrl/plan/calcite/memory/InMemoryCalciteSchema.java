package ai.datasqrl.plan.calcite.memory;

import ai.datasqrl.plan.calcite.memory.table.DataTable;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.schema.AbstractSqrlSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * An schema that holds data in-memory
 */
public class InMemoryCalciteSchema extends AbstractSqrlSchema {

  Map<String, DataTable> dataTableMap = new HashMap<>();

  @Override
  public Table getTable(String table) {
    return dataTableMap.get(table);
  }

  @Override
  public Set<String> getTableNames() {
    return dataTableMap.keySet();
  }

  public void registerDataTable(String name, DataTable table) {
    dataTableMap.put(name, table);
  }

  @Override
  public Expression getExpression(SchemaPlus schemaPlus, String s) {
    return Schemas.subSchemaExpression(schemaPlus, s, this.getClass());
  }

}
