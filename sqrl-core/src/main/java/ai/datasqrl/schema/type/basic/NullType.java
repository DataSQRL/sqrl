package ai.datasqrl.schema.type.basic;

import ai.datasqrl.schema.type.SqmlTypeVisitor;

public class NullType extends AbstractBasicType<NullType> {

  public static final NullType INSTANCE = new NullType();

  @Override
  public String getName() {
    return "NULL";
  }

  @Override
  public BasicType parentType() {
    return NumberType.INSTANCE;
  }

  @Override
  public TypeConversion<NullType> conversion() {
    return new Conversion();
  }

  public static class Conversion extends SimpleBasicType.Conversion<NullType> {

    public Conversion() {
      super(
          NullType.class, s -> new NullType());
    }
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitNullType(this, context);
  }
}
