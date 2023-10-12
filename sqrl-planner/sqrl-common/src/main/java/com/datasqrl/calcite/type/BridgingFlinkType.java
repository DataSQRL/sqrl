package com.datasqrl.calcite.type;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.VectorFunctions;
import com.datasqrl.calcite.Dialect;
import com.datasqrl.function.SqrlFunction;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;

@Getter
public abstract class BridgingFlinkType extends AbstractSqlType implements ForeignType {
  private final Class<?> conversionClass;
  private final Optional<UserDefinedFunction> downcastFlinkFunction;
  private final Optional<UserDefinedFunction> upcastFlinkFunction;
  private final Map<Dialect, String> physicalTypeName;

  public BridgingFlinkType(RelDataType flinkType, Class<?> conversionClass,
      Optional<UserDefinedFunction> downcastFunction, Optional<UserDefinedFunction> upcastFunction,
      Map<Dialect, String> physicalTypeName) {
    this(flinkType.getSqlTypeName(), flinkType.getSqlIdentifier(), flinkType.isNullable(), List.of(),
        flinkType.getComparability(), conversionClass, downcastFunction, upcastFunction, physicalTypeName);
  }

  public BridgingFlinkType(SqlTypeName typeName, SqlIdentifier sqlIdentifier, boolean nullable,
      List<? extends RelDataTypeField> fields, RelDataTypeComparability comparability,
      Class<?> conversionClass,
      Optional<UserDefinedFunction> downcastFunction, Optional<UserDefinedFunction> upcastFunction,
      Map<Dialect, String> physicalTypeName) {
    super(typeName, nullable, List.of());
    this.conversionClass = conversionClass;
    this.downcastFlinkFunction = downcastFunction;
    this.upcastFlinkFunction = upcastFunction;
    this.physicalTypeName = physicalTypeName;
    computeDigest();
  }

  @Override
  public void generateTypeString(StringBuilder stringBuilder, boolean b) {
    stringBuilder.append(this.getDigestName());
  }

  protected abstract String getDigestName();

  @Override
  public boolean equalsSansFieldNames(RelDataType that) {
    return super.equalsSansFieldNames(that);
  }

  public abstract TypeInformation getPhysicalTypeInformation();

  public abstract DataType getFlinkNativeType();

  @Override
  public Optional<SqlFunction> getDowncastFunction() {
    return downcastFlinkFunction
        .map(f->lightweightOp(((SqrlFunction)f).getFunctionName().getCanonical()));
  }

  @Override
  public Optional<SqlFunction> getUpcastFunction() {
    return upcastFlinkFunction
        .map(f->lightweightOp(((SqrlFunction)f).getFunctionName().getCanonical()));
  }
}
