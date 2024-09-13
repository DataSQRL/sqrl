package com.datasqrl.io.schema.avro;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import org.apache.avro.LogicalType;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.types.DataType;

@AllArgsConstructor
public class AvroToRelDataTypeConverter {

  private final ErrorCollector errors;
  private final Map<Schema, RelDataType> typeCache;
  private final boolean legacyTimestampMapping;

  public AvroToRelDataTypeConverter(ErrorCollector errors, boolean legacyTimestampMapping) {
    this(errors, new IdentityHashMap<>(), legacyTimestampMapping);
  }

  //timestamp_mapping.legacy
  public RelDataType convert(Schema schema) {
    validateSchema(schema, NamePath.ROOT);

    DataType dataType = AvroSchemaConverter.convertToDataType(schema.toString(false),
        legacyTimestampMapping);

    TypeFactory typeFactory = TypeFactory.getTypeFactory();

    return typeFactory.createFieldTypeFromLogicalType(
        dataType.getLogicalType());
  }

  private void validateSchema(Schema schema, NamePath path) {
    // Check if the schema has already been processed
    if (typeCache.containsKey(schema)) {
      throw errors.exception(ErrorCode.SCHEMA_ERROR, "Recursive schema's not yet supported: %s", path);
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

        Schema innerSchema = nonNullTypes.get(0);
        validateSchema(innerSchema, path);

        typeCache.put(schema, null);
        break;
      case RECORD:
        // Create a placeholder RelDataType and put it in the cache to handle recursion
        typeCache.put(schema, null);

        for (Field field : schema.getFields()) {
          validateSchema(field.schema(),
              path.concat(Name.system(field.name())));
        }
        break;
      case ARRAY:
        validateSchema(schema.getElementType(), path);
        typeCache.put(schema, null);
        break;
      case MAP:
        validateSchema(schema.getValueType(), path);
        typeCache.put(schema, null);
        break;
      default: // primitives
        validatePrimitive(schema, path);
        typeCache.put(schema, null);
        break;
    }
  }

  private void validatePrimitive(Schema schema, NamePath path) {
    LogicalType logicalType = schema.getLogicalType();
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
