package com.datasqrl.calcite.schema.sql;

import static org.apache.calcite.sql.type.SqlTypeUtil.inCharFamily;
import static org.apache.calcite.sql.type.SqlTypeUtil.isAtomic;
import static org.apache.calcite.sql.type.SqlTypeUtil.isCollection;
import static org.apache.calcite.sql.type.SqlTypeUtil.isRow;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlRowTypeNameSpec;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.sql.parser.type.ExtendedSqlCollectionTypeNameSpec;
import org.apache.flink.sql.parser.type.ExtendedSqlRowTypeNameSpec;
import org.apache.flink.sql.parser.type.SqlMapTypeNameSpec;
import org.apache.flink.sql.parser.type.SqlRawTypeNameSpec;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;
import org.apache.flink.table.types.logical.RawType;

import lombok.experimental.UtilityClass;

@UtilityClass
public class SqlDataTypeSpecBuilder {

  public static RelDataType create(SqlDataTypeSpec typeSpec, RelDataTypeFactory typeFactory) {
    var nullable = typeSpec.getNullable() != null ? typeSpec.getNullable() : true;

    if (typeSpec.getTypeNameSpec() instanceof SqlUserDefinedTypeNameSpec) {
      var udf = (SqlUserDefinedTypeNameSpec) typeSpec.getTypeNameSpec();
      var typeName = udf.getTypeName();
      SqlTypeName sqlTypeName = SqlTypeName.get(typeName.getSimple());
      if (sqlTypeName == null) {
        throw new RuntimeException("Could not find type: "+ typeName.getSimple());
      }

      return typeFactory.createTypeWithNullability(
          typeFactory.createSqlType(sqlTypeName), nullable);
    }

    if (typeSpec.getTypeNameSpec() instanceof SqlBasicTypeNameSpec) {
      var basicTypeNameSpec = (SqlBasicTypeNameSpec) typeSpec.getTypeNameSpec();
      var typeName = basicTypeNameSpec.getTypeName();
      SqlTypeName sqlTypeName = SqlTypeName.get(typeName.getSimple());
      RelDataType sqlType;
      if (basicTypeNameSpec.getPrecision() != -1 && basicTypeNameSpec.getScale() != -1) {
        sqlType =typeFactory.createSqlType(sqlTypeName, basicTypeNameSpec.getPrecision(),
            basicTypeNameSpec.getScale());
      } else if (basicTypeNameSpec.getPrecision() != -1) {
        sqlType =typeFactory.createSqlType(sqlTypeName, basicTypeNameSpec.getPrecision());
      } else {
        sqlType = typeFactory.createSqlType(sqlTypeName);
      }
      return typeFactory.createTypeWithNullability(sqlType, nullable);
    }

    if (typeSpec.getTypeNameSpec() instanceof ExtendedSqlCollectionTypeNameSpec) {
      var collectionTypeNameSpec = (ExtendedSqlCollectionTypeNameSpec) typeSpec.getTypeNameSpec();
      var elementType = create(collectionTypeNameSpec.getElementTypeName(), typeFactory);
      return typeFactory.createTypeWithNullability(
          typeFactory.createArrayType(elementType, -1), nullable);
    }

    if (typeSpec.getTypeNameSpec() instanceof SqlRowTypeNameSpec) {
      var rowTypeNameSpec = (SqlRowTypeNameSpec) typeSpec.getTypeNameSpec();
      var fieldNames = rowTypeNameSpec.getFieldNames();
      var fieldTypeSpecs = rowTypeNameSpec.getFieldTypes();

      var fieldInfoBuilder = typeFactory.builder();
      for (var i = 0; i < fieldNames.size(); i++) {
        var fieldName = fieldNames.get(i);
        var fieldTypeSpec = fieldTypeSpecs.get(i);
        fieldInfoBuilder.add(fieldName.getSimple(), create(fieldTypeSpec, typeFactory));
      }

      return typeFactory.createTypeWithNullability(
          typeFactory.createStructType(fieldInfoBuilder), nullable);
    }
//
//    if (typeSpec.getTypeNameSpec() instanceof SqlRawTypeNameSpec) {
//      SqlRawTypeNameSpec rawTypeNameSpec = (SqlRawTypeNameSpec) typeSpec.getTypeNameSpec();
//      RawType<?> rawType = new RawType<>(
//          (Class<?>) rawTypeNameSpec.getClass().getName(),
//          rawTypeNameSpec.getSerializerString().toString());
//
//      return new RawRelDataType(rawType, typeSpec.getNullable());
//    }

    throw new UnsupportedOperationException("Unsupported type when create RelDataType: " + typeSpec.getTypeNameSpec());
  }

  private static RelDataType create(SqlTypeNameSpec typeNameSpec, RelDataTypeFactory typeFactory) {
    var typeSpec = new SqlDataTypeSpec(typeNameSpec, SqlParserPos.ZERO);
    return create(typeSpec, typeFactory);
  }

  public static SqlDataTypeSpec create(RelDataType type) {
    if (type.getSqlTypeName() == SqlTypeName.ANY) {
      return new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.ANY, SqlParserPos.ZERO), SqlParserPos.ZERO);
    }

    return SqlDataTypeSpecBuilder.convertTypeToSpec(type);
  }

  // Unparses with NOT NULL
  public static SqlDataTypeSpec convertTypeToFlinkSpec(RelDataType type) {
    var charSetName = inCharFamily(type) ? type.getCharset().name() : null;
    return convertTypeToFlinkSpec(type, charSetName, -1);
  }

  public static SqlDataTypeSpec convertTypeToFlinkSpec(RelDataType type, String charSetName, int maxPrecision) {
    var typeName = type.getSqlTypeName();

    assert typeName != null;

    Object typeNameSpec;
    if (!isAtomic(type)) {
      if (isCollection(type)) {
        typeNameSpec = new ExtendedSqlCollectionTypeNameSpec(
            convertTypeToSpec(type.getComponentType()).getTypeNameSpec(), type.isNullable(),
            typeName, true, SqlParserPos.ZERO);
      } else {
        if (type instanceof RawRelDataType) {
          RawType<?> rawType = ((RawRelDataType) type).getRawType();
          typeNameSpec = new SqlRawTypeNameSpec(
              SqlLiteral.createCharString(rawType.getOriginatingClass().getName(),
                  SqlParserPos.ZERO),
              SqlLiteral.createCharString(rawType.getSerializerString(), SqlParserPos.ZERO),
              SqlParserPos.ZERO);
        } else if (!isRow(type)) {
          RelDataType keyType = type.getKeyType();
          RelDataType valueType = type.getValueType();
          var keyTypeSpec = convertTypeToSpec(keyType);
          var valueTypeSpec = convertTypeToSpec(valueType);
          typeNameSpec = new SqlMapTypeNameSpec(
              new SqlDataTypeSpec(keyTypeSpec.getTypeNameSpec(), SqlParserPos.ZERO),
              new SqlDataTypeSpec(valueTypeSpec.getTypeNameSpec(), SqlParserPos.ZERO),
              SqlParserPos.ZERO);
        } else {
          var recordType = (RelRecordType) type;
          var fields = recordType.getFieldList();
          List<SqlIdentifier> fieldNames = fields.stream()
              .map(f -> new SqlIdentifier(f.getName(), SqlParserPos.ZERO))
              .collect(Collectors.toList());
          List fieldTypes = fields.stream().map(f -> convertTypeToSpec(f.getType()))
              .collect(Collectors.toList());
          typeNameSpec = new ExtendedSqlRowTypeNameSpec(SqlParserPos.ZERO, fieldNames, fieldTypes,
              fieldNames.stream().map(e -> (SqlCharStringLiteral) null)
                  .collect(Collectors.toList()), true);
        }
      }
    } else {
      var precision = typeName.allowsPrec() ? type.getPrecision() : -1;
      if (maxPrecision > 0 && precision > maxPrecision) {
        precision = maxPrecision;
      }

      var scale = typeName.allowsScale() ? type.getScale() : -1;
      typeNameSpec = new SqlBasicTypeNameSpec(typeName, precision, scale, charSetName, SqlParserPos.ZERO);
    }

    return new SqlDataTypeSpecNotNull((SqlTypeNameSpec)typeNameSpec, SqlParserPos.ZERO)
        .withNullable(type.isNullable());
  }

  public static SqlDataTypeSpec convertTypeToSpec(RelDataType type) {
    var charSetName = inCharFamily(type) ? type.getCharset().name() : null;
    return convertTypeToFlinkSpec(type, charSetName, -1);
  }

  public class SqlDataTypeSpecNotNull extends SqlDataTypeSpec {


    public SqlDataTypeSpecNotNull(SqlTypeNameSpec typeNameSpec, SqlParserPos pos) {
      super(typeNameSpec, pos);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
      super.unparse(writer, leftPrec, rightPrec);
      if (this.getNullable() != null && this.getNullable()) {
        writer.keyword("NOT NULL");
      }
    }
  }
}
