package com.datasqrl.calcite.type;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.function.SqrlFunction;
import java.lang.reflect.Type;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.type.AbstractSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.apache.flink.table.types.DataType;

@Getter
public abstract class BridgingFlinkType extends AbstractSqlType implements ForeignType {
  protected final Class<?> conversionClass;
  protected final Optional<UserDefinedFunction> downcastFlinkFunction;
  protected final String digestName;

  public BridgingFlinkType(RelDataType flinkType, Class<?> conversionClass,
      Optional<UserDefinedFunction> downcastFunction, String digestName) {
    this(flinkType.getSqlTypeName(), flinkType.isNullable(),
        conversionClass, downcastFunction, digestName);
  }

  public BridgingFlinkType(SqlTypeName typeName, boolean nullable,
      Class<?> conversionClass,
      Optional<UserDefinedFunction> downcastFunction, String digestName) {
    super(typeName, nullable, List.of());
    this.conversionClass = conversionClass;
    this.downcastFlinkFunction = downcastFunction;
    this.digestName = digestName;
    computeDigest();
  }

  public Class<?> getConversionClass() {
    return conversionClass;
  }

  public Optional<UserDefinedFunction> getDowncastFlinkFunction() {
    return downcastFlinkFunction;
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
}
