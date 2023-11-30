package com.datasqrl.schema;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.StreamUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;

@AllArgsConstructor
public class AvroToRelDataTypeConverter {

  private final TypeFactory typeFactory;
  private final ErrorCollector errors;

  public AvroToRelDataTypeConverter(ErrorCollector errors) {
    this(TypeFactory.getTypeFactory(), errors);
  }

  public RelDataType convert(Schema schema) {
    return convertAvroSchemaToCalciteType(schema, NamePath.ROOT);
  }

  private RelDataType convertAvroSchemaToCalciteType(Schema schema, NamePath path) {
    RelDataType relType;
    switch (schema.getType()) {
      case UNION:
        boolean containsNull = schema.getTypes().stream().anyMatch(type -> type.getType().equals(Type.NULL));
        Optional<Schema> innerType;
        try {
          innerType = StreamUtil.getOnlyElement(
              schema.getTypes().stream().filter(type -> !type.getType().equals(Type.NULL)));
        } catch (IllegalArgumentException e) {
          errors.fatal(ErrorCode.SCHEMA_ERROR, "Only AVRO unions with a single non-null type are supported, but found multiple types at: %",path);
          return null;
        }
        if (innerType.isEmpty()) {
          errors.fatal(ErrorCode.SCHEMA_ERROR, "Only AVRO unions with a single non-null type are supported, but found no types at: %",path);
          return null;
        }
        relType = convertAvroSchemaToCalciteType(innerType.get(), path);
        if (relType==null) return null;
        if (containsNull) relType = typeFactory.createTypeWithNullability(relType, true);
        return relType;
      case RECORD:
        List<String> fieldNames = new ArrayList<>();
        List<RelDataType> fieldTypes = new ArrayList<>();
        for (Field field : schema.getFields()) {
          relType = convertAvroSchemaToCalciteType(field.schema(), path.concat(
              Name.system(field.name())));
          if (relType!=null) {
            fieldNames.add(field.name());
            fieldTypes.add(relType);
          }
        }
        if (fieldTypes.isEmpty()) {
          errors.fatal(ErrorCode.SCHEMA_ERROR, "AVRO record does not contain any valid field at: %",path);
          return null;
        }
        return notNull(typeFactory.createStructType(fieldTypes, fieldNames));
      case ARRAY:
        relType = convertAvroSchemaToCalciteType(schema.getElementType(), path);
        if (relType==null) return null;
        return notNull(typeFactory.createArrayType(relType, -1));
      case MAP:
        relType = convertAvroSchemaToCalciteType(schema.getValueType(), path);
        if (relType==null) return null;
        return notNull(typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR), relType));
      default: //primitives
        relType = getPrimitive(schema, path);
        if (relType!=null) relType = notNull(relType);
        return relType;
    }
  }

  private RelDataType getPrimitive(Schema schema, NamePath path) {
    switch (schema.getType()) {
      case FIXED:
        if (logicalTypeEquals(schema, "decimal")) {
          return notNull(typeFactory.createSqlType(SqlTypeName.DECIMAL));
        } else {
          errors.fatal(ErrorCode.SCHEMA_ERROR, "Unrecognized FIXED type in AVRO schema [%s] at: %s", schema, path);
          return null;
        }
      case ENUM:
      case STRING:
        return typeFactory.createSqlType(SqlTypeName.VARCHAR);
      case BYTES:
        return typeFactory.createSqlType(SqlTypeName.VARBINARY);
      case INT:
        if (logicalTypeEquals(schema, "date")) {
          return typeFactory.createSqlType(SqlTypeName.DATE);
        } else if (logicalTypeEquals(schema, "time-millis")) {
          return typeFactory.createSqlType(SqlTypeName.TIME);
        } else {
          return typeFactory.createSqlType(SqlTypeName.INTEGER);
        }
      case LONG:
        if (logicalTypeEquals(schema, "timestamp-millis")) {
          return TypeFactory.makeTimestampType(typeFactory);
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
        errors.fatal(ErrorCode.SCHEMA_ERROR, "NULL not supported as type at: %s", path);
        return null;
      default:
        errors.fatal(ErrorCode.SCHEMA_ERROR, "Unrecognized AVRO Type [%s] at: %s", schema.getType(), path);
        return null;
    }
  }

  private static boolean logicalTypeEquals(Schema schema, String typeName) {
    return schema.getLogicalType()!=null && schema.getLogicalType().getName().equalsIgnoreCase(typeName);
  }


  private RelDataType notNull(RelDataType type) {
    return typeFactory.createTypeWithNullability(type, false);
  }



}
