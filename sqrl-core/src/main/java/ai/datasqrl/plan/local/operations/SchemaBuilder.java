package ai.datasqrl.plan.local.operations;

import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import ai.datasqrl.schema.factory.TableFactory;
import lombok.Getter;

public class SchemaBuilder extends SchemaOpVisitor {

  @Getter
  Schema schema = new Schema();

  private final TableFactory tableFactory = new TableFactory();

  public void apply(SchemaUpdateOp operation) {
    operation.accept(this);
  }

  @Override
  public <T> T visit(AddDatasetOp op) {
    schema.addAll(op.getTables());
    return null;
  }

  @Override
  public <T> T visit(AddQueryOp addQueryOp) {
    schema.add(addQueryOp.getTable());
    return null;
  }

  @Override
  public <T> T visit(AddNestedQueryOp op) {
    tableFactory.assignRelationships(
        op.getRelationshipName(),
        op.getTable(),
        op.getParentTable());
    return null;
  }

  @Override
  public <T> T visit(AddFieldOp addFieldOp) {
    Table table = addFieldOp.getTable();
    table.getFields().add(addFieldOp.getField());
    addFieldOp.getRelNode().ifPresent(table::setHead);
    return null;
  }

  public Schema peek() {
    return schema;
  }

  public Schema build() {
    return schema;
  }
}
