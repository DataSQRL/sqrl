package ai.datasqrl.schema.type.basic;

import ai.datasqrl.schema.type.SqrlTypeVisitor;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class StringType extends AbstractBasicType<String> {

  public static final StringType INSTANCE = new StringType();

  @Override
  public String getName() {
    return "STRING";
  }

  @Override
  public TypeConversion<String> conversion() {
    return new Conversion();
  }

  public static class Conversion implements TypeConversion<String> {

    public Conversion() {
    }

    @Override
    public Set<Class> getJavaTypes() {
      return Collections.singleton(String.class);
    }

    public String convert(Object o) {
      return o.toString();
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      if (fromType instanceof UuidType) {
        return Optional.of(5);
      }
      return Optional.of(30);
    }
  }

  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitStringType(this, context);
  }
}
