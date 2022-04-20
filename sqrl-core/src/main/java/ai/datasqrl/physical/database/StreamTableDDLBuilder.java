package ai.datasqrl.physical.database;

import static ai.datasqrl.physical.database.converters.FlinkTypeToSql.toSql;

import ai.datasqrl.physical.database.ddl.CreateTableDDL;
import ai.datasqrl.physical.database.ddl.DropTableDDL;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Schema.UnresolvedColumn;
import org.apache.flink.table.api.Schema.UnresolvedPhysicalColumn;
import org.apache.flink.table.api.Schema.UnresolvedPrimaryKey;
import org.apache.flink.table.api.TableDescriptor;

public class StreamTableDDLBuilder {

  public List<SqlDDLStatement> create(List<TableDescriptor> createdTables, boolean drop) {
    //1. All streamQueries get created into tables
    List<SqlDDLStatement> statements = new ArrayList<>();
    for (TableDescriptor sink : createdTables) {
      if (drop) {
        DropTableDDL dropTableDDL = new DropTableDDL(sink.getOptions().get("table-name"));
        statements.add(dropTableDDL);
      }

      statements.add(create(sink));
    }

    return statements;
  }

  private CreateTableDDL create(TableDescriptor table) {
    Schema schema = table.getSchema().get();
    List<String> pk;

    if (schema.getPrimaryKey().isPresent()) {
      UnresolvedPrimaryKey key = schema.getPrimaryKey().get();
      pk = key.getColumnNames();
    } else {
      throw new RuntimeException("Unknown primary key");
    }

    List<String> columns = new ArrayList<>();
    for (UnresolvedColumn col : schema.getColumns()) {
      String column = toSql((UnresolvedPhysicalColumn) col);
      columns.add(column);
    }

    return new CreateTableDDL(table.getOptions().get("table-name"), columns, pk);
  }
}
