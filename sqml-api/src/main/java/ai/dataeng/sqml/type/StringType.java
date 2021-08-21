package ai.dataeng.sqml.type;

public class StringType extends ScalarType {

  public static StringType INSTANCE = new StringType();

  public StringType() {
    super("STRING");
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitString(this, context);
  }
}
