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
package com.datasqrl.graphql.server;

import graphql.GraphQLContext;
import graphql.Scalars;
import graphql.execution.CoercedVariables;
import graphql.language.Value;
import graphql.scalars.ExtendedScalars;
import graphql.schema.Coercing;
import graphql.schema.CoercingSerializeException;
import graphql.schema.GraphQLScalarType;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.List;
import java.util.Locale;

public class CustomScalars {

  public static final GraphQLScalarType DOUBLE =
      GraphQLScalarType.newScalar()
          .name("Float")
          .description("A Float with rounding applied")
          .coercing(
              new Coercing() {

                @Override
                public Object serialize(
                    Object dataFetcherResult, GraphQLContext ctx, Locale locale) {

                  if (dataFetcherResult instanceof Double doubleValue) {
                    var bd =
                        new BigDecimal(doubleValue)
                            .setScale(8, RoundingMode.HALF_UP)
                            .stripTrailingZeros();
                    // Convert back to normal readable number
                    return new BigDecimal(bd.toPlainString());
                  } else {
                    return Scalars.GraphQLFloat.getCoercing()
                        .serialize(dataFetcherResult, ctx, locale);
                  }
                }

                @Override
                public Object parseValue(Object input, GraphQLContext ctx, Locale locale) {
                  return Scalars.GraphQLFloat.getCoercing().parseValue(input, ctx, locale);
                }

                @Override
                public Object parseLiteral(
                    Value input, CoercedVariables variables, GraphQLContext ctx, Locale locale) {
                  return Scalars.GraphQLFloat.getCoercing()
                      .parseLiteral(input, variables, ctx, locale);
                }
              })
          .build();

  // Flexible DateTime scalar that handles both full RFC3339 and shorter Flink timestamps
  public static final GraphQLScalarType FLEXIBLE_DATETIME =
      GraphQLScalarType.newScalar()
          .name("DateTime")
          .description(
              "A DateTime scalar that handles both full RFC3339 and shorter timestamp formats")
          .coercing(new FlexibleDateTimeCoercing())
          .build();

  // Extended scalars
  public static final GraphQLScalarType DATE = ExtendedScalars.Date;
  public static final GraphQLScalarType TIME = ExtendedScalars.LocalTime;
  public static final GraphQLScalarType JSON = ExtendedScalars.Json;
  public static final GraphQLScalarType LONG = ExtendedScalars.GraphQLLong;

  public static List<GraphQLScalarType> getExtendedScalars() {
    return List.of(FLEXIBLE_DATETIME, DATE, TIME, JSON, LONG);
  }

  @SuppressWarnings("NullableProblems")
  static class FlexibleDateTimeCoercing implements Coercing<OffsetDateTime, String> {

    private static final Coercing<?, ?> DATE_TIME_COERCING = ExtendedScalars.DateTime.getCoercing();

    // Create flexible formatter that handles multiple timestamp formats
    private static final DateTimeFormatter FLEXIBLE_FORMATTER =
        new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(DateTimeFormatter.ISO_LOCAL_DATE)
            .appendLiteral('T')
            .appendValue(ChronoField.HOUR_OF_DAY, 2)
            .appendLiteral(':')
            .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
            .optionalStart()
            .appendLiteral(':')
            .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .optionalEnd()
            .optionalStart()
            .appendOffsetId()
            .optionalEnd()
            .toFormatter();

    @Override
    public String serialize(Object dataFetcherResult, GraphQLContext ctx, Locale locale) {
      try {
        return (String) DATE_TIME_COERCING.serialize(dataFetcherResult, ctx, locale);

      } catch (CoercingSerializeException e) {
        // Try to normalize if it's a string
        if (dataFetcherResult instanceof String timestampStr) {
          var normalizedTimestamp = normalizeTimestamp(timestampStr);

          if (normalizedTimestamp != null) {
            return (String) DATE_TIME_COERCING.serialize(normalizedTimestamp, ctx, locale);
          }
        }
        throw e;
      }
    }

    @Override
    public OffsetDateTime parseValue(Object input, GraphQLContext ctx, Locale locale) {
      try {
        return (OffsetDateTime) DATE_TIME_COERCING.parseValue(input, ctx, locale);

      } catch (Exception e) {
        // Try to normalize if it's a string
        if (input instanceof String timestampStr) {
          var normalizedTimestamp = normalizeTimestamp(timestampStr);

          if (normalizedTimestamp != null) {
            return (OffsetDateTime) DATE_TIME_COERCING.parseValue(normalizedTimestamp, ctx, locale);
          }
        }
        throw e;
      }
    }

    @Override
    public OffsetDateTime parseLiteral(
        Value<?> input, CoercedVariables variables, GraphQLContext ctx, Locale locale) {

      return (OffsetDateTime) DATE_TIME_COERCING.parseLiteral(input, variables, ctx, locale);
    }

    static String normalizeTimestamp(String timestamp) {
      if (timestamp == null || timestamp.trim().isEmpty()) {
        return timestamp;
      }

      try {
        // Try to parse as OffsetDateTime first (preserves offset if present)
        try {
          var offsetDateTime = OffsetDateTime.parse(timestamp, FLEXIBLE_FORMATTER);
          return offsetDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);

        } catch (DateTimeParseException e) {
          // If no offset, parse as LocalDateTime and add UTC offset
          var parsed = LocalDateTime.parse(timestamp, FLEXIBLE_FORMATTER);
          var offsetDateTime = parsed.atOffset(ZoneOffset.UTC);
          return offsetDateTime.format(DateTimeFormatter.ISO_OFFSET_DATE_TIME);
        }

      } catch (DateTimeParseException e) {
        return null;
      }
    }
  }
}
