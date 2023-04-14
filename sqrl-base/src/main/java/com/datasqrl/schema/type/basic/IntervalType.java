/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.type.basic;

import com.datasqrl.schema.type.SqrlTypeVisitor;

import java.time.Duration;
import java.util.Optional;

public class IntervalType extends AbstractBasicType<Duration> {

  public static final IntervalType INSTANCE = new IntervalType();

  @Override
  public String getName() {
    return "INTERVAL";
  }

  @Override
  public TypeConversion<Duration> conversion() {
    return new Conversion();
  }

  public static class Conversion extends SimpleBasicType.Conversion<Duration> {

    public Conversion() {
      super(Duration.class, s -> Duration.ofMillis(Long.parseLong(s)));
    }

    public Duration convert(Object o) {
      if (o instanceof Duration) {
        return (Duration) o;
      }
      if (o instanceof Float || o instanceof Double) {
        return Duration.ofMillis((long) ((Number) o).doubleValue() * 1000);
      } else if (o instanceof Number) {
        return Duration.ofMillis(((Number) o).longValue());
      }
      throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      if (fromType instanceof FloatType) {
        return Optional.of(40);
      } else if (fromType instanceof IntegerType) {
        return Optional.of(25);
      }
      return Optional.empty();
    }
  }

  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitIntervalType(this, context);
  }
}
