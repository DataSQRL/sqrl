package com.datasqrl.calcite.type;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.flink.FlinkConverter;
import com.datasqrl.json.NativeType;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;

public class UserDefinedBridgingType extends BridgingFlinkType implements PrimitiveType {

  private final NativeType nativeType;
  private final RelDataType flinkRelType;
  private final TypeFactory typeFactory;

  public UserDefinedBridgingType(NativeType nativeType, RelDataType flinkRelType,
      UserDefinedFunction downcastFlinkFunction, TypeFactory typeFactory) {
    super(typeFactory.createType(nativeType.getType()),
        nativeType.getType(), Optional.of(downcastFlinkFunction),
        nativeType.getPhysicalTypeName(Dialect.CALCITE.name()));
    this.nativeType = nativeType;
    this.flinkRelType = flinkRelType;
    this.typeFactory = typeFactory;
  }

  @Override
  public boolean isStruct() {
    return false;
  }

  @Override
  public SqlTypeName getSqlTypeName() {
    return SqlTypeName.STRUCTURED;
  }

  @Override
  public boolean translates(Dialect dialect, RelDataType engineType) {
    if (dialect == Dialect.FLINK) {
      return flinkRelType.equalsSansFieldNames(engineType);
    }

    throw new RuntimeException("Could not translate type");
  }

  @Override
  public String getPhysicalTypeName(Dialect dialect) {
    return nativeType.getPhysicalTypeName(dialect.name());
  }

  @Override
  public RelDataType getEngineType(Dialect dialect) {
    if (dialect == Dialect.FLINK) {
      DataType dataType = DataTypes.of(nativeType.getType())
          .toDataType(FlinkConverter.catalogManager.getDataTypeFactory());
      RelDataType flinkType = FlinkConverter.flinkTypeFactory
          .createFieldTypeFromLogicalType(dataType.getLogicalType());
      return flinkType;
    } else if (dialect == Dialect.POSTGRES) {
      return typeFactory.createSqlType(SqlTypeName.VARCHAR);
    }

    throw new RuntimeException("Unknown dialect during type mapping");
  }

  @Override
  protected String getDigestName() {
    return digestName;
  }

  @Override
  public TypeInformation getPhysicalTypeInformation() {
    return BasicTypeInfo.STRING_TYPE_INFO;
  }

  @Override
  public DataType getFlinkNativeType() {
    return DataTypes.of(nativeType.getType())
        .toDataType(FlinkConverter.catalogManager.getDataTypeFactory());
  }

  @Override
  public String getName() {
    return getDigestName();
  }
}
