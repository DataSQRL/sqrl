package ai.datasqrl.schema.operations;

public interface SqrlOperation {
  public <T> T accept(OperationVisitor visitor);
}
