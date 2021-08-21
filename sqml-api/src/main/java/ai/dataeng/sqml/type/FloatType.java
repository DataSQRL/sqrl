package ai.dataeng.sqml.type;

public class FloatType extends NumberType {

  public static FloatType INSTANCE = new FloatType();

  public FloatType() {
    super("FLOAT");
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitFloat(this, context);
  }

  public Type combine(Type otherType) {
    if (otherType != null && otherType instanceof NumberType) {
      return NumberType.INSTANCE;
    } else {
      return null;
    }
  }
}
