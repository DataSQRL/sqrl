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
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class TimestampType extends AbstractBasicType<Instant> {

  public static final TimestampType INSTANCE = new TimestampType();

  @Override
  public List<String> getNames() {
    return List.of("TIMESTAMP", "DATETIME");
  }

  @Override
  public <R, C> R accept(SqrlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitTimestampType(this, context);
  }

  @Override
  public Conversion conversion() {
    return Conversion.INSTANCE;
  }

  private static class StringParser implements Function<String, Instant> {

    DateTimeFormatter[] formatters = {
      DateTimeFormatter.ISO_INSTANT,
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSX"),
      DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.systemDefault()),
      DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]").withZone(ZoneId.systemDefault())
    };

    @Override
    public Instant apply(String s) {
      for (DateTimeFormatter formatter : formatters) {
        try {
          return formatter.parse(s, Instant::from);
        } catch (DateTimeParseException e) {
          // try next formatter
        }
      }
      throw new IllegalArgumentException();
    }
  }

  public static class Conversion extends SimpleBasicType.Conversion<Instant> {

    private static final Conversion INSTANCE = new Conversion();

    public Conversion() {
      super(Instant.class, new StringParser());
    }

    @Override
    public Instant convert(Object o) {
      if (o instanceof Instant instant) {
        return instant;
      }
      if (o instanceof Number number) {
        return Instant.ofEpochSecond(number.longValue());
      }
      throw new IllegalArgumentException("Invalid type to convert: " + o.getClass());
    }

    @Override
    public Optional<Integer> getTypeDistance(BasicType fromType) {
      if (fromType instanceof BigIntType) {
        return Optional.of(70);
      }
      return Optional.empty();
    }
  }
}
