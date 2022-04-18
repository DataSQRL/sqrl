package ai.datasqrl.physical.database;

import ai.datasqrl.schema.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableDescriptor;

public class SqlDDLGenerator {

  private final Map<Table, TableDescriptor> tables;

  public SqlDDLGenerator(Map<Table, TableDescriptor> tables) {
    this.tables = tables;
  }

  public List<String> generate() {
    List<String> dml = new ArrayList<>();

    for (Map.Entry<Table, TableDescriptor> entry : tables.entrySet()) {

      CreateTableBuilder tableBuilder = new CreateTableBuilder(entry.getValue().getOptions().get("table-name"))
          .addColumns(entry.getValue().getSchema().get(), entry.getKey());
      String sql = tableBuilder.getSQL();
      dml.add(sql);
    }

    return dml;
  }
}
