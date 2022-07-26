package ai.datasqrl;

import ai.datasqrl.plan.local.analyze.StubTable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.schema.AbstractSqrlSchema;
import org.apache.calcite.schema.Table;

public class SqrlSchema extends AbstractSqrlSchema {

  private Map<String, StubTable> tables = new HashMap<>();

  @Override
  public Table getTable(String s) {
    return tables.get(s);
  }

  public void addTable(String name, StubTable table) {
    this.tables.put(name, table);
  }

  @Override
  public Set<String> getTableNames() {
    return tables.keySet();
  }
}
