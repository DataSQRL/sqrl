package ai.datasqrl.plan.local.operations;

public abstract class SchemaOpVisitor {

  public abstract <T> T visit(AddDatasetOp op);

  public abstract <T> T visit(AddQueryOp addQueryOp);

  public abstract <T> T visit(AddColumnOp addColumnOp);

}
