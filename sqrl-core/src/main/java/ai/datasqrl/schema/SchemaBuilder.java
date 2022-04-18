package ai.datasqrl.schema;

import ai.datasqrl.plan.ImportTable;
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
    for (ImportTable entry :
        op.getResult().getImportedPaths()) {
      Table table = tableFactory.create(entry.getTableName(), entry.getRelNode(),
          entry.getFields());
      schema.add(table);
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
