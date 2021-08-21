package ai.dataeng.sqml.type;

public class DateTimeType extends ScalarType {

  public static DateTimeType INSTANCE = new DateTimeType();

  public DateTimeType() {
    super("DATE");
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitDateTime(this, context);
  }
}
