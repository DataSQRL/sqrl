package org.apache.calcite.jdbc;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Delegate for the type factory
 */
public class SqrlTypeFactory extends SqlTypeFactoryImpl {

  public SqrlTypeFactory() {
    super(RelDataTypeSystem.DEFAULT);
  }

  @Override
  public RelDataType createSqlType(SqlTypeName typeName) {
    return super.createSqlType(typeName);
  }

  @Override
  public RelDataType createSqlType(SqlTypeName typeName, int precision) {
    return super.createSqlType(typeName, precision);
  }

  @Override
  public RelDataType createSqlType(SqlTypeName typeName, int precision, int scale) {
    return super.createSqlType(typeName, precision, scale);
  }

  @Override
  protected RelDataType canonize(StructKind kind, List<String> names, List<RelDataType> types,
      boolean nullable) {
    return super.canonize(kind, names, types, nullable);
  }

  @Override
  protected RelDataType canonize(RelDataType type) {
    return super.canonize(type);
  }
}
