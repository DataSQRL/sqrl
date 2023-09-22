package com.datasqrl.calcite.type;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.flink.FlinkConverter;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;

public class VectorType extends BridgingFlinkType implements PrimitiveType {

  private final DataType flinkDataType;
  private final RelDataType flinkRelType;
  private final TypeFactory typeFactory;

  public VectorType(DataType flinkDataType, RelDataType flinkRelType,
      UserDefinedFunction downcastFlinkFunction, TypeFactory typeFactory) {
    super(typeFactory.createType(FlinkVectorType.class),
        VectorType.class, Optional.of(downcastFlinkFunction),
        Optional.empty(),
        Map.of(Dialect.POSTGRES, "VECTOR"));
    this.flinkDataType = flinkDataType;
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
    if (dialect == Dialect.POSTGRES) {
      return "VECTOR";
    }
    throw new RuntimeException("Could not get physical name of type");
  }

  @Override
  public RelDataType getEngineType(Dialect dialect) {
    if (dialect == Dialect.FLINK) {
      DataType dataType = DataTypes.of(FlinkVectorType.class)
          .toDataType(FlinkConverter.catalogManager.getDataTypeFactory());
      RelDataType flinkType = FlinkConverter.flinkTypeFactory
          .createFieldTypeFromLogicalType(dataType.getLogicalType());
      return flinkType;
    } else if (dialect == Dialect.POSTGRES) {
      return typeFactory.createArrayType(typeFactory.createSqlType(SqlTypeName.BOOLEAN), -1);
    }

    throw new RuntimeException("Unknown dialect during type mapping");
  }

  @Override
  public TypeInformation getPhysicalTypeInformation() {
    return BasicArrayTypeInfo.DOUBLE_ARRAY_TYPE_INFO;
  }

  @Override
  public DataType getFlinkNativeType() {
    return DataTypes.ARRAY(DataTypes.DOUBLE());
  }
}
