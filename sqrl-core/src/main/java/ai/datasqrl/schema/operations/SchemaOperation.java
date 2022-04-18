package ai.datasqrl.schema.operations;

/**
 *
 */
public interface SchemaOperation {
  public <T> T accept(OperationVisitor visitor);
}
