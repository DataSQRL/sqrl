package com.datasqrl.calcite.type;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.VectorFunctions;
import com.datasqrl.calcite.Dialect;
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

@Getter
public class BridgingFlinkType extends AbstractSqlType implements DelegatingDataType, NonnativeType  {
  private final Class<?> conversionClass;
  private final Optional<SqlFunction> downcastFunction;
  private final Optional<SqlFunction> upcastFunction;
  private final Map<Dialect, String> physicalTypeName;

  public BridgingFlinkType(RelDataType flinkType, Class<?> conversionClass,
      Optional<SqlFunction> downcastFunction, Optional<SqlFunction> upcastFunction,
      Map<Dialect, String> physicalTypeName) {
    this(flinkType.getSqlTypeName(), flinkType.getSqlIdentifier(), flinkType.isNullable(), List.of(),
        flinkType.getComparability(), conversionClass, downcastFunction, upcastFunction, physicalTypeName);
  }

  public BridgingFlinkType(SqlTypeName typeName, SqlIdentifier sqlIdentifier, boolean nullable,
      List<? extends RelDataTypeField> fields, RelDataTypeComparability comparability,
      Class<?> conversionClass,
      Optional<SqlFunction> downcastFunction, Optional<SqlFunction> upcastFunction,
      Map<Dialect, String> physicalTypeName) {
    super(typeName, nullable, List.of());
    this.conversionClass = conversionClass;
    this.downcastFunction = downcastFunction;
    this.upcastFunction = upcastFunction;
    this.physicalTypeName = physicalTypeName;
    computeDigest();
  }

  @Override
  public String getPhysicalType(Dialect dialect) {
    return physicalTypeName.get(dialect);
  }

  @Override
  public Optional<SqlFunction> getDowncastFunction() {
    return Optional.of(lightweightOp(VectorFunctions.VEC_TO_DOUBLE.getFunctionName().getCanonical()));
  }

  @Override
  public Optional<SqlFunction> getUpcastFunction(){
    return Optional.empty();
  }

  @Override
  public void generateTypeString(StringBuilder stringBuilder, boolean b) {
    stringBuilder.append("vector");
  }

  @Override
  public boolean equalsSansFieldNames(RelDataType that) {
    return super.equalsSansFieldNames(that);
  }
}
