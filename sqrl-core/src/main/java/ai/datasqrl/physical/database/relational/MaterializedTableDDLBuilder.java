package ai.datasqrl.physical.database.relational;

import ai.datasqrl.physical.database.relational.ddl.*;
import ai.datasqrl.plan.calcite.util.RelToSql;
import ai.datasqrl.plan.global.IndexSelection;
import ai.datasqrl.plan.global.OptimizedDAG;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class MaterializedTableDDLBuilder {

  public List<SqlDDLStatement> createTables(Collection<OptimizedDAG.DatabaseSink> createdTables, boolean drop) {
    List<SqlDDLStatement> statements = new ArrayList<>();
    for (OptimizedDAG.DatabaseSink table : createdTables) {
      if (drop) {
        DropTableDDL dropTableDDL = new DropTableDDL(table.getNameId());
        statements.add(dropTableDDL);
      }
      statements.add(createTable(table));
    }

    return statements;
  }

  public List<SqlDDLStatement> createIndexes(Collection<IndexSelection> indexes, boolean drop) {
    List<SqlDDLStatement> statements = new ArrayList<>();
    for (IndexSelection index : indexes) {
      Preconditions.checkArgument(index.prune().equals(index));
      if (drop) {
        DropIndexDDL dropIndex = new DropIndexDDL(index.getName(),index.getTable().getNameId());
        statements.add(dropIndex);
      }
      statements.add(createIndex(index));
    }
    return statements;
  }

  private CreateTableDDL createTable(OptimizedDAG.DatabaseSink table) {
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

  private CreateIndexDDL createIndex(IndexSelection index) {
    CreateIndexDDL.Type type = CreateIndexDDL.Type.HASH;
    if (index.getRemainingIndexColumns().stream().anyMatch(c -> c.getType() == IndexSelection.Type.INEQUALITY)) {
      type = CreateIndexDDL.Type.BTREE;
    }
    List<String> fieldNames = index.getTable().getRowType().getFieldNames();
    List<String> columns = index.getColumns().stream().map(c -> fieldNames.get(c.getColumnIndex()))
            .collect(Collectors.toList());
    return new CreateIndexDDL(index.getName(), index.getTable().getNameId(), columns, type);
  }
}
