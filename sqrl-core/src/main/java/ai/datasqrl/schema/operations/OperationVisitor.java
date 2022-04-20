package ai.datasqrl.schema.operations;

public abstract class OperationVisitor {

  public abstract <T> T visit(AddDatasetOp op);

  public abstract <T> T visit(AddQueryOp addQueryOp);

  public abstract <T> T visit(AddColumnOp addColumnOp);

}
