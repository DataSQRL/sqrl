package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.Relation;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.ImportTable;
import ai.datasqrl.schema.Relationship.Multiplicity;
import ai.datasqrl.schema.Relationship.Type;
import ai.datasqrl.schema.operations.AddDatasetOp;
import ai.datasqrl.schema.operations.OperationVisitor;
import ai.datasqrl.schema.operations.SchemaOperation;
import lombok.Getter;

public class SchemaBuilder extends OperationVisitor {
  @Getter
  Schema schema = new Schema();

  private final TableFactory2 tableFactory = new TableFactory2();

  public void apply(SchemaOperation operation) {
    operation.accept(this);
  }

  @Override
  public <T> T visit(AddDatasetOp op) {
    for (ImportTable entry : op.getResult().getImportedPaths()) {
      NamePath namePath = entry.getTableName();
      if (namePath.getLength() == 1) {
        Table table = tableFactory.create(namePath.getFirst(), namePath, entry.getRelNode(),
            entry.getFields(),
            entry.getPrimaryKey(), entry.getParentPrimaryKey());
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
        Table newTable = tableFactory.create(namePath.getLast(), namePath, entry.getRelNode(), entry.getFields(),
            entry.getPrimaryKey(), entry.getParentPrimaryKey());
        Relationship relationship = new Relationship(namePath.getLast(),
            table, newTable, Type.CHILD, Multiplicity.MANY);
        table.addField(relationship);
      }
    }

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
