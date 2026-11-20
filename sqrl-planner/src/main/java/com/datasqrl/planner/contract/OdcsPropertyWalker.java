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
package com.datasqrl.planner.contract;

import com.datasqrl.calcite.type.RelDataTypeWalker;
import com.datasqrl.calcite.type.TypeKind;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.planner.contract.model.OdcsMap;
import com.datasqrl.planner.contract.model.OdcsProperty;
import com.datasqrl.planner.util.Documented.Lookup;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.TimestampType;

/**
 * Maps a type to a property of a data contract. The property has no name: the caller names the
 * property of a column, and the walker names the fields of a row.
 *
 * <p>The logical types depend on the ODCS version: v3.0 has no {@code timestamp} and {@code time}
 * types, and only v3.2 has the {@code map} type.
 */
@RequiredArgsConstructor
public class OdcsPropertyWalker extends RelDataTypeWalker<OdcsProperty> {

  static final String STRING = "string";
  static final String INTEGER = "integer";
  static final String NUMBER = "number";
  static final String BOOLEAN = "boolean";
  static final String DATE = "date";
  static final String TIME = "time";
  static final String TIMESTAMP = "timestamp";
  static final String OBJECT = "object";
  static final String ARRAY = "array";
  static final String MAP = "map";

  private final OdcsVersion version;

  /** The docs of the columns of the table, in which the docs of nested fields are looked up */
  private final Lookup columnDocs;

  @Override
  protected Optional<OdcsProperty> visitScalar(RelDataType type, TypeKind kind, NamePath path) {
    var property = OdcsProperty.builder().required(required(type));

    switch (kind) {
      case BOOLEAN -> property.logicalType(BOOLEAN);
      case TINYINT -> property.logicalType(INTEGER).logicalTypeOptions(format("i8"));
      case SMALLINT -> property.logicalType(INTEGER).logicalTypeOptions(format("i16"));
      case INTEGER -> property.logicalType(INTEGER).logicalTypeOptions(format("i32"));
      case BIGINT -> property.logicalType(INTEGER).logicalTypeOptions(format("i64"));
      // ODCS has no precision and scale, the physical type keeps them
      case DECIMAL -> property.logicalType(NUMBER);
      case FLOAT -> property.logicalType(NUMBER).logicalTypeOptions(format("f32"));
      case DOUBLE -> property.logicalType(NUMBER).logicalTypeOptions(format("f64"));
      case STRING -> property.logicalType(STRING).logicalTypeOptions(maxLength(type));
      case BINARY -> property.logicalType(STRING).logicalTypeOptions(format("binary"));
      case INTERVAL -> property.logicalType(STRING);
      case DATE -> property.logicalType(DATE);
      case TIME -> property.logicalType(version.hasTimestamp() ? TIME : STRING);
      case TIME_LTZ -> withTimezone(property, version.hasTimestamp() ? TIME : STRING, true);
      case TIMESTAMP -> withTimezone(property, version.hasTimestamp() ? TIMESTAMP : DATE, false);
      case TIMESTAMP_LTZ -> withTimezone(property, version.hasTimestamp() ? TIMESTAMP : DATE, true);
      // JSON has no fixed fields
      case JSON -> property.logicalType(OBJECT);
      // The vector type of ODCS requires the dimensions, which the type doesn't have
      case VECTOR ->
          property
              .logicalType(ARRAY)
              .items(
                  OdcsProperty.builder()
                      .logicalType(NUMBER)
                      .logicalTypeOptions(format("f64"))
                      .physicalType("DOUBLE")
                      .required(true)
                      .build());
      default -> {
        // A type without a logical type is only described by its physical type
      }
    }

    return Optional.of(property.physicalType(physicalType(type, kind)).build());
  }

  @Override
  protected Optional<OdcsProperty> visitRow(
      RelDataType type, List<Field<OdcsProperty>> fields, NamePath path) {
    // The path starts with the name of the column
    var fieldDocs = columnDocs.nested(path, 0);
    var properties =
        fields.stream()
            .map(
                field ->
                    field.value().toBuilder()
                        .name(field.field().getName())
                        .description(fieldDocs.getDocsFor(field.field().getName()).orElse(null))
                        .build())
            .toList();

    return Optional.of(
        OdcsProperty.builder()
            .logicalType(OBJECT)
            .physicalType(type.getSqlTypeName().getName())
            .required(required(type))
            .properties(properties)
            .build());
  }

  @Override
  protected Optional<OdcsProperty> visitArray(
      RelDataType type, OdcsProperty element, NamePath path) {
    return Optional.of(
        OdcsProperty.builder()
            .logicalType(ARRAY)
            .physicalType(type.getSqlTypeName().getName())
            .required(required(type))
            .items(element)
            .build());
  }

  @Override
  protected Optional<OdcsProperty> visitMap(RelDataType type, NamePath path) {
    var property =
        OdcsProperty.builder()
            .physicalType(type.getSqlTypeName().getName())
            .required(required(type));
    var key = walk(type.getKeyType(), path);
    var value = walk(type.getValueType(), path);

    if (version.hasMap() && key.isPresent() && value.isPresent()) {
      return Optional.of(
          property.logicalType(MAP).map(new OdcsMap(key.get(), value.get())).build());
    }
    return Optional.of(property.logicalType(OBJECT).build());
  }

  /** {@code true} if the value cannot be null, otherwise not set, since false is the default. */
  private static Boolean required(RelDataType type) {
    return type.isNullable() ? null : Boolean.TRUE;
  }

  private static Map<String, Object> format(String format) {
    return Map.of("format", format);
  }

  private static Map<String, Object> maxLength(RelDataType type) {
    var length = type.getPrecision();
    return length > 0 && length < Integer.MAX_VALUE ? Map.of("maxLength", length) : null;
  }

  private OdcsProperty.OdcsPropertyBuilder withTimezone(
      OdcsProperty.OdcsPropertyBuilder property, String logicalType, boolean timezone) {
    property.logicalType(logicalType);
    // v3.0 has neither the timestamp type nor its timezone option
    return version.hasTimestamp()
        ? property.logicalTypeOptions(Map.of("timezone", timezone))
        : property;
  }

  /**
   * The type as Flink prints it, without its nullability and without the time attribute that the
   * planner tracks on a timestamp, e.g. {@code DECIMAL(10, 2)} or {@code TIMESTAMP_LTZ(3)}.
   */
  private static String physicalType(RelDataType type, TypeKind kind) {
    if (kind == TypeKind.JSON || kind == TypeKind.VECTOR) {
      return kind.name();
    }
    var logicalType = FlinkTypeFactory.toLogicalType(type).copy(true);
    if (logicalType instanceof LocalZonedTimestampType timestamp) {
      logicalType = new LocalZonedTimestampType(true, timestamp.getPrecision());
    } else if (logicalType instanceof TimestampType timestamp) {
      logicalType = new TimestampType(true, timestamp.getPrecision());
    }
    return logicalType.asSummaryString();
  }
}
