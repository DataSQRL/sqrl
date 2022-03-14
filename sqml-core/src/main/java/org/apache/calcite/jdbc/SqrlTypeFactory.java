package org.apache.calcite.jdbc;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.FieldPath;
import ai.dataeng.sqml.planner.Relationship.Multiplicity;
import ai.dataeng.sqml.type.basic.BasicType;
import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.StructKind;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqrlBasicSqlType;

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

  public RelDataType createSqrlType(FieldPath field) {
    if (field.getLastField() instanceof Column) {
      return new SqrlBasicSqlType(toSqlTypeName(((Column)field.getLastField()).getType()), false,
          Multiplicity.ONE);
    } else {
      return super.createStructType(StructKind.PEEK_FIELDS_DEFAULT, List.of(), List.of());
    }
  }

  private static SqlTypeName toSqlTypeName(BasicType column) {
    switch (column.getName()) {
      case "INTEGER":
        return SqlTypeName.INTEGER;
      case "BOOLEAN":
        return SqlTypeName.BOOLEAN;
      case "STRING":
        return SqlTypeName.VARCHAR;
      case "UUID":
        return SqlTypeName.VARBINARY;
      case "FLOAT":
        return SqlTypeName.FLOAT;
      case "DOUBLE":
        return SqlTypeName.DOUBLE;
      case "TIMESTAMP":
        return SqlTypeName.TIMESTAMP;
      case "DATETIME":
        return SqlTypeName.TIMESTAMP;
      //todo: remaining
    }
    throw new RuntimeException(String.format(
        "Unrecognized type %s", column.getClass().getName()));
  }
}
