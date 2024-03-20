/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.type.basic;

import com.datasqrl.schema.type.SqrlTypeVisitor;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class DoubleType extends AbstractBasicType<Double> {

  public static final DoubleType INSTANCE = new DoubleType();

  @Override
  public List<String> getNames() {
    return List.of("DOUBLE", "FLOAT");
  }

  @Override
  public TypeConversion<Double> conversion() {
    return new Conversion();
  }

  public static class Conversion extends SimpleBasicType.Conversion<Double> {

    private static final Set<Class> FLOAT_CLASSES = Set.of(Float.class, Double.class);

    public Conversion() {
      super(Double.class, Double::valueOf);
    }

    @Override
    public Set<Class> getJavaTypes() {
      return FLOAT_CLASSES;
    }

    public Double convert(Object o) {
      if (o instanceof Double) {
        return Double.valueOf((Double)o);
      }
      if (o instanceof Number) {
        return Double.valueOf(((Number) o).doubleValue());
      }
      if (o instanceof Boolean) {
        return ((Boolean) o).booleanValue() ? 1d : 0d;
      }
      if (o instanceof Duration) {
        return ((Duration) o).toMillis() / 1000.0d;
      }
      throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      if (fromType instanceof BigIntType) {
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
    return visitor.visitDoubleType(this, context);
  }
}
