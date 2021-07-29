package ai.dataeng.sqml.schema;

public abstract class AbstractField {
  public abstract <R, C> R accept(SchemaVisitor<R, C> visitor, C context);
  public abstract String getName();
}
