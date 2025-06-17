/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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

    private static final Set<Class> INT_CLASSES =
        Set.of(Integer.class, Long.class, Byte.class, Short.class);

    public Conversion() {
      super(Long.class, s -> Long.parseLong(s));
    }

    @Override
    public Set<Class> getJavaTypes() {
      return INT_CLASSES;
    }

    @Override
    public Long convert(Object o) {
      if (o instanceof Long long1) {
        return long1;
      }
      if (o instanceof Number number) {
        return number.longValue();
      }
      if (o instanceof Boolean boolean1) {
        return boolean1 ? 1L : 0L;
      }
      if (o instanceof Duration duration) {
        return duration.toMillis();
      }
      if (o instanceof Instant instant) {
        return instant.getEpochSecond();
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

  @Override
  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitBigIntType(this, context);
  }
}
