package ai.datasqrl.schema.operations;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.Column;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.factory.TableFactory;
import lombok.Getter;

public class OperationExecutor extends OperationVisitor {
  @Getter
  Schema schema = new Schema();

  private final TableFactory tableFactory = new TableFactory();

  public void apply(SchemaOperation operation) {
    operation.accept(this);
  }

  @Override
  public <T> T visit(AddDatasetOp op) {
    for (ImportTable entry : op.getImportedPaths()) {
      tableFactory.createTable(schema, entry.getTableName(), entry.getFields(), entry.getRelNode(),
          entry.getPrimaryKey(), entry.getParentPrimaryKey());
    }
    return null;
  }

  @Override
  public <T> T visit(AddQueryOp addQueryOp) {
    tableFactory.createTable(schema, addQueryOp.getNamePath(),
        addQueryOp.getFieldNames(),
        addQueryOp.getRelNode(),
        addQueryOp.getPrimaryKeys(),
        addQueryOp.getParentPrimaryKeys()
    );

    return null;
  }

  @Override
  public <T> T visit(AddColumnOp addColumnOp) {
    Name columnName = addColumnOp.getName();
    NamePath pathToTable = addColumnOp.getPathToTable();

    Table baseTable = schema.getByName(pathToTable.getFirst()).get();
    Table table = baseTable.walk(pathToTable.popFirst()).get();

    int version = 0;
    if (table.getField(columnName) != null) {
      version = table.getField(columnName).getVersion() + 1;
    }
    table.addField(Column.createTemp(columnName, null, table, version));
    table.setRelNode(addColumnOp.getRelNode());
    table.setPrimaryKey(addColumnOp.getPrimaryKeys());
    table.setParentPrimaryKey(addColumnOp.getParentPrimaryKeys());
    return null;
  }

  public Schema peek() {
    return schema;
  }

  public Schema build() {
    return schema;
  }
}
