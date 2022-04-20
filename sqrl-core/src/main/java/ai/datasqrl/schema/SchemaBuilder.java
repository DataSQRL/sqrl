package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.ImportTable;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.Relationship.Type;
import ai.datasqrl.schema.operations.AddColumnOp;
import ai.datasqrl.schema.operations.AddDatasetOp;
import ai.datasqrl.schema.operations.AddQueryOp;
import ai.datasqrl.schema.operations.OperationVisitor;
import ai.datasqrl.schema.operations.SchemaOperation;
import java.util.List;
import java.util.Set;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

public class SchemaBuilder extends OperationVisitor {
  @Getter
  Schema schema = new Schema();

  private final TableFactory2 tableFactory = new TableFactory2();

  public void apply(SchemaOperation operation) {
    operation.accept(this);
  }

  @Override
  public <T> T visit(AddDatasetOp op) {
    for (ImportTable entry : op.getImportedPaths()) {
      createTable(entry.getTableName(), entry.getFields(), entry.getRelNode(),
          entry.getPrimaryKey(), entry.getParentPrimaryKey());
    }
    return null;
  }

  private void createTable(NamePath tableName, List<Name> fields, RelNode relNode, Set<Integer> primaryKey, Set<Integer> parentPrimaryKey) {
      NamePath namePath = tableName;
      if (namePath.getLength() == 1) {
        Table table = tableFactory.create(namePath.getFirst(), namePath, relNode,
            fields,
            primaryKey, parentPrimaryKey, null);
        schema.add(table);
      } else {
        Table table = schema.getByName(namePath.get(0)).get();
        Name[] names = namePath.getNames();
        for (int i = 1; i < names.length - 1; i++) {
          Name name = names[i];
          Field field = table.getField(name);
          if (names.length - 2 != i) {
            table = ((Relationship) field).getToTable();
          }
        }
        Table newTable = tableFactory.create(namePath.getLast(), namePath, relNode, fields,
            primaryKey, parentPrimaryKey, table);
        Relationship relationship = new Relationship(namePath.getLast(),
            table, newTable, Type.CHILD, Multiplicity.MANY);
        table.addField(relationship);
      }
    }

  @Override
  public <T> T visit(AddQueryOp addQueryOp) {
    createTable(addQueryOp.getNamePath(),
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

  /**
   *
   */
  public Schema build() {
    return schema;
  }
}
