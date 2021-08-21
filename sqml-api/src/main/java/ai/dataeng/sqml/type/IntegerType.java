package ai.dataeng.sqml.type;

public class IntegerType extends NumberType {

  public static IntegerType INSTANCE = new IntegerType();

  public IntegerType() {
    super("INTEGER");
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitInteger(this, context);
  }

  public Type combine(Type otherType) {
    if (otherType != null && otherType instanceof NumberType) {
      return NumberType.INSTANCE;
    } else {
      return null;
    }
  }
}
