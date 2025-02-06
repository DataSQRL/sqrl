/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.type.basic;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.datasqrl.schema.type.SqrlTypeVisitor;

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

  private static class StringParser implements Function<String,Instant> {

    DateTimeFormatter[] formatters = {
        DateTimeFormatter.ISO_INSTANT,
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSX"),
        DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.systemDefault()),
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSS]")
            .withZone(ZoneId.systemDefault())
    };

    @Override
    public Instant apply(String s) {
      for (DateTimeFormatter formatter : formatters) {
        try {
          return formatter.parse(s, Instant::from);
        } catch (DateTimeParseException e) {
          //try next formatter
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
