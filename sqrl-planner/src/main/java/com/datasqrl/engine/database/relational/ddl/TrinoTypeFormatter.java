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
package com.datasqrl.engine.database.relational.ddl;

import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;

/** Formats Calcite types using Trino's DDL grammar. */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class TrinoTypeFormatter {

  public static String format(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    if (typeName == null) {
      throw new IllegalArgumentException("Trino does not support type: " + type);
    }

    return switch (typeName) {
      case CHAR -> "CHAR" + precision(type);
      case VARCHAR ->
          type.getPrecision() == Integer.MAX_VALUE ? "VARCHAR" : "VARCHAR" + precision(type);
      case BINARY, VARBINARY -> "VARBINARY";
      case DECIMAL -> "DECIMAL(%d, %d)".formatted(type.getPrecision(), type.getScale());
      case FLOAT, REAL -> "REAL";
      case TIMESTAMP -> "TIMESTAMP" + precision(type);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE -> "TIMESTAMP" + precision(type) + " WITH TIME ZONE";
      case TIME -> "TIME" + precision(type);
      case TIME_WITH_LOCAL_TIME_ZONE -> "TIME" + precision(type) + " WITH TIME ZONE";
      case ARRAY, MULTISET -> "ARRAY(" + format(type.getComponentType()) + ")";
      case MAP -> "MAP(" + format(type.getKeyType()) + ", " + format(type.getValueType()) + ")";
      case ROW -> "ROW(" + formatFields(type) + ")";
      default -> typeName.getName();
    };
  }

  private static String formatFields(RelDataType type) {
    return type.getFieldList().stream()
        .map(TrinoTypeFormatter::formatField)
        .collect(Collectors.joining(", "));
  }

  private static String formatField(RelDataTypeField field) {
    return quote(field.getName()) + " " + format(field.getType());
  }

  private static String quote(String identifier) {
    return "\"" + identifier.replace("\"", "\"\"") + "\"";
  }

  private static String precision(RelDataType type) {
    return type.getPrecision() == RelDataType.PRECISION_NOT_SPECIFIED
        ? ""
        : "(" + type.getPrecision() + ")";
  }
}
