package ai.dataeng.sqml.type;

public class UuidType extends ScalarType {

  public static UuidType INSTANCE = new UuidType();

  public UuidType() {
    super("UUID");
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitUuid(this, context);
  }
}
