/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.type.basic;

import com.datasqrl.io.schema.flexible.type.SqrlTypeVisitor;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class BigIntType extends AbstractBasicType<Long> {

  public static final BigIntType INSTANCE = new BigIntType();

  @Override
  public List<String> getNames() {
    return List.of("BIGINT", "INTEGER");
  }

  @Override
  public TypeConversion<Long> conversion() {
    return new Conversion();
  }

  public static class Conversion extends SimpleBasicType.Conversion<Long> {

    private static final Set<Class> INT_CLASSES = Set.of(Integer.class, Long.class,
        Byte.class, Short.class);

    public Conversion() {
      super(Long.class, s -> Long.parseLong(s));
    }

    @Override
    public Set<Class> getJavaTypes() {
      return INT_CLASSES;
    }

    public Long convert(Object o) {
      if (o instanceof Long) {
        return (Long) o;
      }
      if (o instanceof Number) {
        return ((Number) o).longValue();
      }
      if (o instanceof Boolean) {
        return ((Boolean) o).booleanValue() ? 1L : 0L;
      }
      if (o instanceof Duration) {
        return ((Duration) o).toMillis();
      }
      if (o instanceof Instant) {
        return ((Instant) o).getEpochSecond();
      }
      throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      if (fromType instanceof DoubleType) {
        return Optional.of(12);
      } else if (fromType instanceof BooleanType) {
        return Optional.of(4);
      } else if (fromType instanceof IntervalType) {
        return Optional.of(45);
      } else if (fromType instanceof TimestampType) {
        return Optional.of(95);
      }
      return Optional.empty();
    }
  }

  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitBigIntType(this, context);
  }
}
