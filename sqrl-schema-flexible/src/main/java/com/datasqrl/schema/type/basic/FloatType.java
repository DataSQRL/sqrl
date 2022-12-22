/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.type.basic;

import com.datasqrl.schema.type.SqrlTypeVisitor;
import com.google.common.collect.ImmutableSet;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.Optional;
import java.util.Set;

public class FloatType extends AbstractBasicType<BigDecimal> {

  public static final FloatType INSTANCE = new FloatType();

  @Override
  public String getName() {
    return "FLOAT";
  }

  @Override
  public TypeConversion<BigDecimal> conversion() {
    return new Conversion();
  }

  public static class Conversion extends SimpleBasicType.Conversion<BigDecimal> {

    private static final Set<Class> FLOAT_CLASSES = ImmutableSet.of(Float.class, Double.class);

    public Conversion() {
      super(BigDecimal.class, s -> new BigDecimal(s));
    }

    @Override
    public Set<Class> getJavaTypes() {
      return FLOAT_CLASSES;
    }

    public BigDecimal convert(Object o) {
      if (o instanceof Double) {
        return BigDecimal.valueOf((Double)o);
      }
      if (o instanceof Number) {
        return BigDecimal.valueOf(((Number) o).doubleValue());
      }
      if (o instanceof Boolean) {
        return ((Boolean) o).booleanValue() ? BigDecimal.ONE : BigDecimal.ZERO;
      }
      if (o instanceof Duration) {
        return BigDecimal.valueOf(((Duration) o).toMillis())
            .divide(BigDecimal.valueOf(1000.0));
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
