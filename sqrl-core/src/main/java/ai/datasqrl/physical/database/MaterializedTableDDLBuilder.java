package ai.datasqrl.physical.database;

import ai.datasqrl.physical.database.ddl.CreateTableDDL;
import ai.datasqrl.physical.database.ddl.DropTableDDL;
import ai.datasqrl.physical.database.ddl.SqlDDLStatement;
import ai.datasqrl.plan.calcite.util.RelToSql;
import ai.datasqrl.plan.global.OptimizedDAG;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.List;

public class MaterializedTableDDLBuilder {

  public List<SqlDDLStatement> createTables(List<OptimizedDAG.TableSink> createdTables, boolean drop) {
    List<SqlDDLStatement> statements = new ArrayList<>();
    for (OptimizedDAG.TableSink table : createdTables) {
      if (drop) {
        DropTableDDL dropTableDDL = new DropTableDDL(table.getNameId());
        statements.add(dropTableDDL);
      }

      statements.add(create(table));
    }

    return statements;
  }

  private CreateTableDDL create(OptimizedDAG.TableSink table) {
    List<String> pk = new ArrayList<>();
    List<String> columns = new ArrayList<>();

    List<RelDataTypeField> fields = table.getRowType().getFieldList();
    for (int i = 0; i < fields.size(); i++) {
      RelDataTypeField field = fields.get(i);
      String column = RelToSql.toSql(field);
      columns.add(column);
      if (i < table.getNumPrimaryKeys()) {
        pk.add(field.getName());
      }
    }
    return new CreateTableDDL(table.getNameId(), columns, pk);
  }
}
