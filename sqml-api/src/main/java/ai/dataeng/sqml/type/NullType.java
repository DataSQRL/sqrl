package ai.dataeng.sqml.type;

public class NullType extends ScalarType {

  public static NullType INSTANCE = new NullType();

  public NullType() {
    super("NULL");
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitNull(this, context);
  }
}
