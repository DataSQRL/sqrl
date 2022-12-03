package com.datasqrl.engine.database.relational;

import com.datasqrl.engine.database.relational.ddl.*;
import com.datasqrl.plan.calcite.util.RelToSql;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.OptimizedDAG;
import org.apache.calcite.rel.type.RelDataTypeField;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

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

  public List<SqlDDLStatement> createIndexes(Collection<IndexDefinition> indexes, boolean drop) {
    List<SqlDDLStatement> statements = new ArrayList<>();
    for (IndexDefinition index : indexes) {
      if (drop) {
        DropIndexDDL dropIndex = new DropIndexDDL(index.getName(),index.getTableId());
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

  private CreateIndexDDL createIndex(IndexDefinition index) {
    List<String> columns = index.getColumnNames();
    return new CreateIndexDDL(index.getName(), index.getTableId(), columns, index.getType());
  }
}
