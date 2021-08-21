package ai.dataeng.sqml.type;

public class UnknownType extends Type {

  public static UnknownType INSTANCE = new UnknownType();

  public UnknownType() {
    super("UNKNOWN");
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitUnknown(this, context);
  }
}
