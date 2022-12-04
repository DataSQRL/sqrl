package com.datasqrl.schema.type.basic;

import com.datasqrl.schema.type.SqrlTypeVisitor;
import com.google.common.collect.ImmutableSet;

import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public class FloatType extends AbstractBasicType<Double> {

  public static final FloatType INSTANCE = new FloatType();

  @Override
  public String getName() {
    return "FLOAT";
  }

  @Override
  public TypeConversion<Double> conversion() {
    return new Conversion();
  }

  public static class Conversion extends SimpleBasicType.Conversion<Double> {

    private static final Set<Class> FLOAT_CLASSES = ImmutableSet.of(Float.class, Double.class);

    public Conversion() {
      super(Double.class, s -> Double.parseDouble(s));
    }

    @Override
    public Set<Class> getJavaTypes() {
      return FLOAT_CLASSES;
    }

    public Double convert(Object o) {
      if (o instanceof Double) {
        return (Double) o;
      }
      if (o instanceof Number) {
        return ((Number) o).doubleValue();
      }
      if (o instanceof Boolean) {
        return ((Boolean) o).booleanValue() ? 1.0 : 0.0;
      }
      if (o instanceof Duration) {
        return ((Duration) o).toMillis() / 1000.0;
      }
      throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      if (fromType instanceof IntegerType) {
        return Optional.of(3);
      } else if (fromType instanceof BooleanType) {
        return Optional.of(6);
      } else if (fromType instanceof IntervalType) {
        return Optional.of(55);
      }
      return Optional.empty();
    }
  }


  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitFloatType(this, context);
  }
}
