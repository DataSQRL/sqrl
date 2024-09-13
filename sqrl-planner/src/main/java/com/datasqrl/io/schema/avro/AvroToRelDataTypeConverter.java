package com.datasqrl.io.schema.avro;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

@AllArgsConstructor
public class AvroToRelDataTypeConverter {

  private final TypeFactory typeFactory;
  private final ErrorCollector errors;
  private final Map<Schema, RelDataType> typeCache;

  public AvroToRelDataTypeConverter(ErrorCollector errors) {
    this(TypeFactory.getTypeFactory(), errors, new IdentityHashMap<>());
  }

  public RelDataType convert(Schema schema) {
    return convertAvroSchemaToCalciteType(schema, NamePath.ROOT);
  }

  private RelDataType convertAvroSchemaToCalciteType(Schema schema, NamePath path) {
    // Check if the schema has already been processed
    if (typeCache.containsKey(schema)) {
      throw errors.exception(ErrorCode.SCHEMA_ERROR, "Recursive schema's not yet supported: %s", path);
    }

    switch (schema.getType()) {
      case UNION:
        boolean containsNull = schema.getTypes().stream()
            .anyMatch(type -> type.getType() == Type.NULL);

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
        RelDataType unionType = convertAvroSchemaToCalciteType(innerSchema, path);
        if (containsNull) {
          unionType = typeFactory.createTypeWithNullability(unionType, true);
        }
        typeCache.put(schema, unionType);
        return unionType;
      case RECORD:
        // Create a placeholder RelDataType and put it in the cache to handle recursion
        List<String> fieldNames = new ArrayList<>();
        List<RelDataType> fieldTypes = new ArrayList<>();
        RelDataType placeholder = typeFactory.createStructType(fieldTypes, fieldNames);
        typeCache.put(schema, placeholder);

        for (Field field : schema.getFields()) {
          RelDataType fieldType = convertAvroSchemaToCalciteType(field.schema(),
              path.concat(Name.system(field.name())));
          fieldNames.add(field.name());
          fieldTypes.add(fieldType);
        }
        RelDataType recordType = notNull(typeFactory.createStructType(fieldTypes, fieldNames));

        typeCache.put(schema, recordType);
        return recordType;
      case ARRAY:
        RelDataType elementType = convertAvroSchemaToCalciteType(schema.getElementType(), path);
        RelDataType arrayType = notNull(typeFactory.createArrayType(elementType, -1));
        typeCache.put(schema, arrayType);
        return arrayType;
      case MAP:
        RelDataType valueType = convertAvroSchemaToCalciteType(schema.getValueType(), path);
        RelDataType mapType = notNull(typeFactory.createMapType(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), valueType));
        typeCache.put(schema, mapType);
        return mapType;
      default: // primitives
        RelDataType primitiveType = getPrimitive(schema, path);
        Preconditions.checkNotNull(primitiveType, "Avro primitive type return null.");
        primitiveType = notNull(primitiveType);
        typeCache.put(schema, primitiveType);
        return primitiveType;
    }
  }

  private RelDataType getPrimitive(Schema schema, NamePath path) {
    LogicalType logicalType = schema.getLogicalType();
    switch (schema.getType()) {
      case FIXED:
        if (logicalType instanceof LogicalTypes.Decimal) {
          LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
          int precision = decimalType.getPrecision();
          int scale = decimalType.getScale();
          return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        } else {
          // Map FIXED type to VARBINARY with length
          return typeFactory.createSqlType(SqlTypeName.VARBINARY, schema.getFixedSize());
        }
      case ENUM:
        // Map ENUM to VARCHAR with length of maximum symbol length
        List<String> symbols = schema.getEnumSymbols();
        int maxLength = symbols.stream().mapToInt(String::length).max().orElse(1);
        return typeFactory.createSqlType(SqlTypeName.VARCHAR, maxLength);
      case STRING:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case BYTES:
        if (logicalType instanceof LogicalTypes.Decimal) {
          LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
          int precision = decimalType.getPrecision();
          int scale = decimalType.getScale();
          return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
        } else {
          return typeFactory.createSqlType(SqlTypeName.VARBINARY);
        }
      case INT:
        if (logicalType == LogicalTypes.date()) {
          return typeFactory.createSqlType(SqlTypeName.DATE);
        } else if (logicalType == LogicalTypes.timeMillis()) {
          return typeFactory.createSqlType(SqlTypeName.TIME, 3); // milliseconds precision
        } else {
          return typeFactory.createSqlType(SqlTypeName.INTEGER);
        }
      case LONG:
        if (logicalType == LogicalTypes.timestampMillis()) {
          return typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 3); // milliseconds precision
        } else if (logicalType == LogicalTypes.timestampMicros()) {
          return typeFactory.createSqlType(SqlTypeName.TIMESTAMP, 6); // microseconds precision
        } else if (logicalType == LogicalTypes.timeMicros()) {
          //Note: Flink only supports precision 3, this is converted in the RelDataTypeSystem
          // so even though this gets passed 6, the resulting precision will be 3.
          return typeFactory.createSqlType(SqlTypeName.TIME, 6); // microseconds precision
        } else {
          return typeFactory.createSqlType(SqlTypeName.BIGINT);
        }
      case FLOAT:
        return typeFactory.createSqlType(SqlTypeName.FLOAT);
      case DOUBLE:
        return typeFactory.createSqlType(SqlTypeName.DOUBLE);
      case BOOLEAN:
        return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
      case NULL:
        return typeFactory.createSqlType(SqlTypeName.NULL);
      default:
        throw errors.exception(ErrorCode.SCHEMA_ERROR, "Unrecognized AVRO Type [%s] at: %s",
            schema.getType(), path);
    }
  }

  private RelDataType notNull(RelDataType type) {
    return typeFactory.createTypeWithNullability(type, false);
  }

}
