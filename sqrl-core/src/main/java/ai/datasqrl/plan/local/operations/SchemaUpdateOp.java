package ai.datasqrl.plan.local.operations;

/**
 * An operation that modifies the schema
 */
public interface SchemaUpdateOp {

  <T> T accept(SchemaOpVisitor visitor);
}
