/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.type.basic;

import com.datasqrl.io.schema.flexible.type.SqrlTypeVisitor;

import java.time.Duration;
import java.util.List;
import java.util.Optional;

public class IntervalType extends AbstractBasicType<Duration> {

  public static final IntervalType INSTANCE = new IntervalType();

  @Override
  public List<String> getNames() {
    return List.of("INTERVAL");
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
      if (o instanceof Duration duration) {
        return duration;
      }
      if (o instanceof Float || o instanceof Double) {
        return Duration.ofMillis((long) ((Number) o).doubleValue() * 1000);
      } else if (o instanceof Number number) {
        return Duration.ofMillis(number.longValue());
      }
      throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      if (fromType instanceof DoubleType) {
        return Optional.of(40);
      } else if (fromType instanceof BigIntType) {
        return Optional.of(25);
      }
      return Optional.empty();
    }
  }

  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitIntervalType(this, context);
  }
}
