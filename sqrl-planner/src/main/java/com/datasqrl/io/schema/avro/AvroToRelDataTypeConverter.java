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
package com.datasqrl.io.schema.avro;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.planner.plan.schema.TimeIndicatorRelDataType;
import org.apache.flink.table.types.logical.RowType;

@AllArgsConstructor
public class AvroToRelDataTypeConverter {
  private final ErrorCollector errors;
  private final Set<Schema> visitedSchemas;
  private final boolean legacyTimestampMapping;

  public AvroToRelDataTypeConverter(ErrorCollector errors, boolean legacyTimestampMapping) {
    this(errors, Collections.newSetFromMap(new IdentityHashMap<>()), legacyTimestampMapping);
  }

  public RelDataType convert(Schema schema) {
    validateSchema(schema, NamePath.ROOT, new HashSet<>()); // recursion stack

    var dataType = AvroSchemaConverter.convertToDataType(schema.toString(), legacyTimestampMapping);
    var typeFactory = TypeFactory.getTypeFactory();

    return typeFactory.createFieldTypeFromLogicalType(dataType.getLogicalType());
  }

  public static Schema convert2Avro(RelDataType rowType, List<String> selectedFields) {
    // Convert rowType to remove ROWTIME markers before Avro conversion
    var typeFactory = TypeFactory.getTypeFactory();
    var builder = typeFactory.builder();

    for (var field : rowType.getFieldList()) {
      // Apply selectedFields filter if provided
      if (selectedFields != null
          && !selectedFields.isEmpty()
          && !selectedFields.contains(field.getName())) {
        continue;
      }
      // Convert ROWTIME to base timestamp type
      var fieldType = field.getType();
      if (fieldType instanceof TimeIndicatorRelDataType) {
        var baseType =
            typeFactory.createSqlType(fieldType.getSqlTypeName(), fieldType.getPrecision());
        fieldType = typeFactory.createTypeWithNullability(baseType, fieldType.isNullable());
      }
      builder.add(field.getName(), fieldType);
    }
    var filteredRowType = builder.build();

    var logicalType = TypeFactory.toLogicalType(filteredRowType);
    if (!(logicalType instanceof RowType convertedRowType)) {
      throw new IllegalArgumentException(
          "AvroSchemaConverter expects a ROW type; got: " + logicalType);
    }

    return AvroSchemaConverter.convertToSchema(convertedRowType);
  }

  private void validateSchema(Schema schema, NamePath path, Set<Schema> recursionStack) {
    if (visitedSchemas.contains(schema)) {
      return; // already validated
    }

    if (!recursionStack.add(schema)) {
      throw errors.exception(
          ErrorCode.SCHEMA_ERROR, "Cyclic-recursive schema reference detected at: %s.", path);
    }

    try {
      switch (schema.getType()) {
        case UNION:
          List<Schema> nonNullTypes = new ArrayList<>();
          for (Schema memberSchema : schema.getTypes()) {
            if (memberSchema.getType() != Type.NULL) {
              nonNullTypes.add(memberSchema);
            }
          }

          if (nonNullTypes.size() != 1) {
            throw errors.exception(
                ErrorCode.SCHEMA_ERROR,
                "Only AVRO unions with a single non-null type are supported, but found %d non-null types at: %s",
                nonNullTypes.size(),
                path);
          }

          Schema innerSchema = nonNullTypes.get(0);
          validateSchema(innerSchema, path, recursionStack);
          break;

        case RECORD:
          for (Field field : schema.getFields()) {
            validateSchema(field.schema(), path.concat(Name.system(field.name())), recursionStack);
          }
          break;

        case ARRAY:
          validateSchema(schema.getElementType(), path, recursionStack);
          break;

        case MAP:
          validateSchema(schema.getValueType(), path, recursionStack);
          break;

        default:
          validatePrimitive(schema, path);
          break;
      }
    } finally {
      recursionStack.remove(schema);
      visitedSchemas.add(schema); // mark as validated
    }
  }

  private void validatePrimitive(Schema schema, NamePath path) {
    switch (schema.getType()) {
      case FIXED:
      case ENUM:
      case STRING:
      case BYTES:
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
      case NULL:
        return;
      default:
        throw errors.exception(
            ErrorCode.SCHEMA_ERROR, "Unrecognized AVRO Type [%s] at: %s", schema.getType(), path);
    }
  }
}
