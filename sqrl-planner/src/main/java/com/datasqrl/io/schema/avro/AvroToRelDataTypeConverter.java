package com.datasqrl.io.schema.avro;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class AvroToRelDataTypeConverter {

  private final ErrorCollector errors;
  private final Set<Schema> processedSchemas;
  private final boolean legacyTimestampMapping;

  public AvroToRelDataTypeConverter(ErrorCollector errors, boolean legacyTimestampMapping) {
    this(errors, Collections.newSetFromMap(new IdentityHashMap<>()), legacyTimestampMapping);
  }

  public RelDataType convert(Schema schema) {
    validateSchema(schema, NamePath.ROOT);

    var dataType = AvroSchemaConverter.convertToDataType(schema.toString(false),
        legacyTimestampMapping);

    var typeFactory = TypeFactory.getTypeFactory();

    return typeFactory.createFieldTypeFromLogicalType(
        dataType.getLogicalType());
  }

  private void validateSchema(Schema schema, NamePath path) {
    // Check if the schema has already been processed
    if (!processedSchemas.add(schema)) {
      throw errors.exception(ErrorCode.SCHEMA_ERROR, "Recursive schemas are not supported: %s", path);
    }

    switch (schema.getType()) {
      case UNION:
        List<Schema> nonNullTypes = new ArrayList<>();
        for (Schema memberSchema : schema.getTypes()) {
          if (memberSchema.getType() != Type.NULL) {
            nonNullTypes.add(memberSchema);
          }
        }

        if (nonNullTypes.size() != 1) {
          throw errors.exception(ErrorCode.SCHEMA_ERROR,
              "Only AVRO unions with a single non-null type are supported, but found %d non-null types at: %s",
              nonNullTypes.size(), path);
        }

        var innerSchema = nonNullTypes.getFirst();
        validateSchema(innerSchema, path);
        break;
      case RECORD:
        for (Field field : schema.getFields()) {
          validateSchema(field.schema(),
              path.concat(Name.system(field.name())));
        }
        break;
      case ARRAY:
        validateSchema(schema.getElementType(), path);
        break;
      case MAP:
        validateSchema(schema.getValueType(), path);
        break;
      default: // primitives
        validatePrimitive(schema, path);
        break;
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
        throw errors.exception(ErrorCode.SCHEMA_ERROR, "Unrecognized AVRO Type [%s] at: %s",
            schema.getType(), path);
    }
  }
}
