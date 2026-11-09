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
package com.datasqrl.server.stream;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SerializationFeature;
import graphql.GraphQLContext;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.impl.JsonUtil;
import io.vertx.core.json.jackson.DatabindCodec;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.impl.Utils;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;
import lombok.SneakyThrows;

/**
 * Writes the GraphQL response for a {@link StreamPlan} row by row, producing the same bytes as
 * {@code Row::toJson}, the column property fetchers, scalar coercion and the Vert.x JSON codec do
 * on the non-streaming path. Output accumulates until {@link #drain()} hands it over.
 */
public final class RowJsonEncoder {

  private static final ObjectWriter WRITER =
      DatabindCodec.mapper().writer().without(SerializationFeature.FLUSH_AFTER_WRITE_VALUE);

  private final StreamPlan plan;
  private final GraphQLContext context;
  private final Locale locale;
  private final ByteArrayOutputStream output = new ByteArrayOutputStream();
  private final JsonGenerator generator;
  private final Object[] values;
  private ColumnLookup[] lookups;

  @SneakyThrows
  public RowJsonEncoder(StreamPlan plan, GraphQLContext context, Locale locale) {
    this.plan = plan;
    this.context = context;
    this.locale = locale;
    this.values = new Object[plan.columns().size()];
    this.generator = DatabindCodec.mapper().createGenerator(output);
    generator.writeStartObject();
    generator.writeFieldName("data");
    generator.writeStartObject();
    generator.writeFieldName(plan.responseKey());
    generator.writeStartArray();
  }

  /** Coerces the whole row before writing any of it, so a failing row leaves no partial object. */
  @SneakyThrows
  public void writeRow(Row row) {
    if (lookups == null) {
      lookups =
          plan.columns().stream()
              .map(column -> ColumnLookup.of(row, column.columnName()))
              .toArray(ColumnLookup[]::new);
    }
    var columns = plan.columns();
    for (var i = 0; i < values.length; i++) {
      values[i] = serialize(columns.get(i), lookups[i].fetch(row));
    }
    generator.writeStartObject();
    for (var i = 0; i < values.length; i++) {
      generator.writeFieldName(columns.get(i).responseKey());
      write(values[i]);
    }
    generator.writeEndObject();
  }

  @SneakyThrows
  public void finish() {
    generator.writeEndArray();
    generator.writeEndObject();
    generator.writeEndObject();
  }

  /** Closes the rows written so far and reports {@code cause} as the response's error. */
  @SneakyThrows
  public void fail(Throwable cause) {
    generator.writeEndArray();
    generator.writeEndObject();
    generator.writeFieldName("errors");
    WRITER.writeValue(generator, List.of(plan.error(cause).toSpecification()));
    generator.writeEndObject();
  }

  public int bufferedBytes() {
    return output.size() + generator.getOutputBuffered();
  }

  @SneakyThrows
  public Buffer drain() {
    generator.flush();
    var chunk = Buffer.buffer(output.toByteArray());
    output.reset();
    return chunk;
  }

  private Object serialize(StreamedColumn column, Object value) {
    if (value == null) {
      return requireNullable(column, column.nonNull());
    }
    if (!column.list()) {
      return coerce(column, column.nonNull(), value);
    }
    if (!(value instanceof Iterable<?> items)) {
      throw new IllegalStateException(
          "Column '%s' is not a list: %s".formatted(column.columnName(), value));
    }
    var serialized = new ArrayList<>();
    for (var item : items) {
      serialized.add(
          item == null
              ? requireNullable(column, column.itemNonNull())
              : coerce(column, column.itemNonNull(), item));
    }
    return serialized;
  }

  private Object coerce(StreamedColumn column, boolean nonNull, Object value) {
    var serialized = column.scalar().getCoercing().serialize(value, context, locale);
    return serialized == null ? requireNullable(column, nonNull) : serialized;
  }

  private static Object requireNullable(StreamedColumn column, boolean nonNull) {
    if (nonNull) {
      throw new IllegalStateException(
          "Non-null field '%s' was null".formatted(column.responseKey()));
    }
    return null;
  }

  private void write(Object value) throws IOException {
    if (value == null) {
      generator.writeNull();
    } else if (value instanceof String string) {
      generator.writeString(string);
    } else if (value instanceof Integer number) {
      generator.writeNumber(number);
    } else if (value instanceof Long number) {
      generator.writeNumber(number);
    } else if (value instanceof Boolean bool) {
      generator.writeBoolean(bool);
    } else {
      WRITER.writeValue(generator, value);
    }
  }

  /**
   * Mirrors {@code CaseInsensitiveJsonDataFetcher} over {@code Row::toJson}: the exactly named
   * column wins unless null, then any other column matching the name case-insensitively.
   */
  private record ColumnLookup(int exact, int[] caseInsensitive) {

    static ColumnLookup of(Row row, String name) {
      var exact = -1;
      var others = IntStream.builder();
      for (var i = 0; i < row.size(); i++) {
        var column = row.getColumnName(i);
        if (name.equals(column)) {
          exact = i;
        } else if (name.equalsIgnoreCase(column)) {
          others.add(i);
        }
      }
      return new ColumnLookup(exact, others.build().toArray());
    }

    Object fetch(Row row) {
      if (exact >= 0) {
        var value = JsonUtil.wrapJsonValue(Utils.toJson(row.getValue(exact)));
        if (value != null) {
          return value;
        }
      }
      for (var index : caseInsensitive) {
        var value = Utils.toJson(row.getValue(index));
        if (value != null) {
          return value;
        }
      }
      return null;
    }
  }
}
