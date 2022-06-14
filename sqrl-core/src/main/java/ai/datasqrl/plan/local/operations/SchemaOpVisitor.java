package ai.datasqrl.plan.local.operations;

public abstract class SchemaOpVisitor {

  public abstract <T> T visit(AddImportedTablesOp op);

  public abstract <T> T visit(AddTableOp op);

  public abstract <T> T visit(AddNestedTableOp op);

  public abstract <T> T visit(AddFieldOp op);
}
