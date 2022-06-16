package ai.datasqrl.plan.calcite;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.type.SqlTypeName;

public class SqrlTypeSystem implements RelDataTypeSystem {
  public static final SqrlTypeSystem INSTANCE = new SqrlTypeSystem();

  public static final int TIMESTAMP_PRECISION = 3;

  @Override
  public int getMaxScale(final SqlTypeName typeName) {
    return RelDataTypeSystem.DEFAULT.getMaxScale(typeName);
  }

  @Override
  public int getDefaultPrecision(final SqlTypeName typeName) {
    switch (typeName) {
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TIMESTAMP_PRECISION;
      default:
        return RelDataTypeSystem.DEFAULT.getDefaultPrecision(typeName);
    }
  }

  @Override
  public int getMaxPrecision(final SqlTypeName typeName) {
    return RelDataTypeSystem.DEFAULT.getMaxPrecision(typeName);
  }

  @Override
  public int getMaxNumericScale() {
    return RelDataTypeSystem.DEFAULT.getMaxNumericScale();
  }

  @Override
  public int getMaxNumericPrecision() {
    return RelDataTypeSystem.DEFAULT.getMaxNumericPrecision();
  }

  @Override
  public String getLiteral(final SqlTypeName typeName, final boolean isPrefix) {
    return RelDataTypeSystem.DEFAULT.getLiteral(typeName, isPrefix);
  }

  @Override
  public boolean isCaseSensitive(final SqlTypeName typeName) {
    return RelDataTypeSystem.DEFAULT.isCaseSensitive(typeName);
  }

  @Override
  public boolean isAutoincrement(final SqlTypeName typeName) {
    return RelDataTypeSystem.DEFAULT.isAutoincrement(typeName);
  }

  @Override
  public int getNumTypeRadix(final SqlTypeName typeName) {
    return RelDataTypeSystem.DEFAULT.getNumTypeRadix(typeName);
  }

  @Override
  public RelDataType deriveSumType(final RelDataTypeFactory typeFactory,
      final RelDataType argumentType) {
    if (SqlTypeName.INT_TYPES.contains(argumentType.getSqlTypeName())) {
      return typeFactory.createSqlType(SqlTypeName.BIGINT);
    } else {
      return typeFactory.createSqlType(SqlTypeName.DOUBLE);
    }
  }

  @Override
  public RelDataType deriveAvgAggType(
      final RelDataTypeFactory typeFactory,
      final RelDataType argumentType) {
    return RelDataTypeSystem.DEFAULT.deriveAvgAggType(typeFactory, argumentType);
  }

  @Override
  public RelDataType deriveCovarType(
      final RelDataTypeFactory typeFactory,
      final RelDataType arg0Type,
      final RelDataType arg1Type
  ) {
    return RelDataTypeSystem.DEFAULT.deriveCovarType(typeFactory, arg0Type, arg1Type);
  }

  @Override
  public RelDataType deriveFractionalRankType(final RelDataTypeFactory typeFactory) {
    return RelDataTypeSystem.DEFAULT.deriveFractionalRankType(typeFactory);
  }

  @Override
  public RelDataType deriveRankType(final RelDataTypeFactory typeFactory) {
    return RelDataTypeSystem.DEFAULT.deriveRankType(typeFactory);
  }

  @Override
  public boolean isSchemaCaseSensitive() {
    return RelDataTypeSystem.DEFAULT.isSchemaCaseSensitive();
  }

  @Override
  public boolean shouldConvertRaggedUnionTypesToVarying() {
    return true;
  }
}
