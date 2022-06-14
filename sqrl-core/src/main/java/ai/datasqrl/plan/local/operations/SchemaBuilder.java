package ai.datasqrl.plan.local.operations;

import ai.datasqrl.plan.local.BundleTableFactory;
import ai.datasqrl.schema.Schema;
import ai.datasqrl.schema.Table;
import lombok.Getter;

public class SchemaBuilder extends SchemaOpVisitor {

  @Getter
  private final BundleTableFactory tableFactory;

  @Getter
  Schema schema = new Schema();

  public SchemaBuilder(BundleTableFactory tableFactory) {
    this.tableFactory = tableFactory;
  }

  public void apply(SchemaUpdateOp operation) {
    operation.accept(this);
  }

  @Override
  public <T> T visit(AddImportedTablesOp op) {
    schema.addAll(op.getTables());
    return null;
  }

  @Override
  public <T> T visit(AddTableOp addTableOp) {
    schema.add(addTableOp.getTable());
    return null;
  }

  @Override
  public <T> T visit(AddNestedTableOp op) {
    tableFactory.createParentChildRelationship(
        op.getRelationshipName(),
        op.getTable(),
        op.getParentTable(),
        op.getMultiplicity());
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

  @Override
  public String toString() {
    return schema.toString();
  }
}
