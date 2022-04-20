package ai.datasqrl.schema.operations;

/**
 * An operation that modifies the schema
 */
public interface SchemaOperation {
  <T> T accept(OperationVisitor visitor);
}
