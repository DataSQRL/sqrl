package ai.dataeng.sqml.execution;

import ai.dataeng.sqml.execution.sql.util.CreateTableBuilder;
import ai.dataeng.sqml.parser.Table;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.flink.table.api.TableDescriptor;

public class SqlGenerator {

  private final Map<Table, TableDescriptor> tables;

  public SqlGenerator(Map<Table, TableDescriptor> tables) {
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
