package ai.datasqrl.plan.local.operations;

import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.TableTimestamp;
import lombok.Getter;

/**
 * SchemaUpdatePlanner creates the schema pieces and this schema builder applies them
 */
public class SchemaBuilder implements SchemaOpVisitor {

  @Getter
  Schema schema = new Schema();

  public void apply(SchemaUpdateOp op) {
    op.accept(this);
  }

  @Override
  public <T> T visit(AddColumnOp op) {
    Table table = op.getTable();
    table.getFields().add(op.getColumn());
    if (op.isTimestamp()) table.getTimestamp().update(op.getColumn(), TableTimestamp.Status.DEFINED);
    return null;
  }

  @Override
  public <T> T visit(AddJoinDeclarationOp op) {
    Table table = op.getTable();
    table.getFields().add(op.getRelationship());
    return null;
  }

  @Override
  public <T> T visit(AddNestedTableOp op) {
    /* no-op, nested tables are added to the schema in AddJoinDeclarationOp */
    return null;
  }

  @Override
  public <T> T visit(AddRootTableOp op) {
    schema.add(op.getTable());
    return null;
  }

  @Override
  public <T> T visit(MultipleUpdateOp op) {
    for (SchemaUpdateOp o : op.getOps()) {
      o.accept(this);
    }
    return null;
  }

  @Override
  public <T> T visit(ScriptTableImportOp op) {
    throw new RuntimeException("tbd");
  }

  @Override
  public <T> T visit(SourceTableImportOp op) {
    schema.add(op.getRootTable());
    return null;
  }

  public Schema peek() {
    return schema;
  }

  public Schema build() {
    return schema;
  }

  @Override
  public String toString() {
    return schema.toString();
  }
}
