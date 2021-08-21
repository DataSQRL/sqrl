package ai.dataeng.sqml.type;

public class BooleanType extends ScalarType {

  public static BooleanType INSTANCE = new BooleanType();

  public BooleanType() {
    super("BOOLEAN");
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitBoolean(this, context);
  }
}
