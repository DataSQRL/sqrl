package ai.datasqrl.plan.local.operations;

public abstract class SchemaOpVisitor {

  public abstract <T> T visit(AddDatasetOp op);

  public abstract <T> T visit(AddQueryOp op);

  public abstract <T> T visit(AddNestedQueryOp op);

  public abstract <T> T visit(AddFieldOp op);
}
