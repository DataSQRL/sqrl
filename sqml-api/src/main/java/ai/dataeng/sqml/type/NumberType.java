package ai.dataeng.sqml.type;

public class NumberType extends ScalarType {

  public static NumberType INSTANCE = new NumberType();

  public NumberType(String name) {
    super(name);
  }

  public NumberType() {
    this("NUMBER");
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitNumber(this, context);
  }

  public Type combine(Type otherType) {
    if (otherType != null && otherType instanceof NumberType) {
      return this;
    } else {
      return null;
    }
  }
}
