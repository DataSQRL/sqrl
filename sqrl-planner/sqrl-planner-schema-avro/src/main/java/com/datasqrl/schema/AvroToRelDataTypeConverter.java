package com.datasqrl.schema;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.StreamUtil;
import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

  private static final Map<Schema.Type, SqlTypeName> TYPE_MAPPING = ImmutableMap.<Schema.Type, SqlTypeName>builder()
      .put(Type.INT, SqlTypeName.INTEGER)
    .put(Type.STRING, SqlTypeName.VARCHAR)
    .put(Type.BYTES, SqlTypeName.VARBINARY)
    .put(Type.BOOLEAN, SqlTypeName.BOOLEAN)
    .put(Type.LONG, SqlTypeName.BIGINT)
    .put(Type.DOUBLE, SqlTypeName.DOUBLE)
    .put(Type.FLOAT, SqlTypeName.FLOAT)
    .build();

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
      case ENUM:
        return notNull(typeFactory.createSqlType(SqlTypeName.VARCHAR));
      case ARRAY:
        relType = convertAvroSchemaToCalciteType(schema.getElementType(), path);
        if (relType==null) return null;
        return notNull(typeFactory.createArrayType(relType, -1));
      case MAP:
        relType = convertAvroSchemaToCalciteType(schema.getValueType(), path);
        if (relType==null) return null;
        return notNull(typeFactory.createMapType(typeFactory.createSqlType(SqlTypeName.VARCHAR), relType));
      case FIXED:
        return notNull(typeFactory.createSqlType(SqlTypeName.DECIMAL));
      default:
        SqlTypeName typeName = TYPE_MAPPING.get(schema.getType());
        if (typeName==null) {
          errors.fatal(ErrorCode.SCHEMA_ERROR, "Encountered unknown AVRO type [%s] at: %s", schema.getType(), path);
          return null;
        }
        return notNull(typeFactory.createSqlType(typeName));
    }
  }

  private RelDataType notNull(RelDataType type) {
    return typeFactory.createTypeWithNullability(type, false);
  }



}
