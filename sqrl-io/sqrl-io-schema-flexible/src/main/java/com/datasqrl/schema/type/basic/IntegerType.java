/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.type.basic;

import com.datasqrl.schema.type.SqrlTypeVisitor;
import com.google.common.collect.ImmutableSet;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class IntegerType extends AbstractBasicType<Integer> {

  public static final IntegerType INSTANCE = new IntegerType();

  @Override
  public List<String> getNames() {
    return List.of("INTEGER");
  }

  @Override
  public TypeConversion<Integer> conversion() {
    return new Conversion();
  }

  public static class Conversion extends SimpleBasicType.Conversion<Integer> {

    private static final Set<Class> INT_CLASSES = ImmutableSet.of(Integer.class, Byte.class, Short.class);

    public Conversion() {
      super(Integer.class, s -> Integer.parseInt(s));
    }

    @Override
    public Set<Class> getJavaTypes() {
      return INT_CLASSES;
    }

    public Integer convert(Object o) {
      if (o instanceof Integer) {
        return (Integer) o;
      }
      if (o instanceof Number) {
        return ((Number) o).intValue();
      }
      if (o instanceof Boolean) {
        return ((Boolean) o).booleanValue() ? 1 : 0;
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
    return visitor.visitIntegerType(this, context);
  }
}
