package com.datasqrl.calcite.type;

import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;

@Getter
public class BridgingFlinkType extends ObjectSqlType implements DelegatingDataType, PrimitiveTypeAlias {
  private final Class<?> conversionClass;
  private final SqlFunction downcastFunction;
  private final SqlFunction upcastFunction;

  public BridgingFlinkType(RelDataType flinkType, Class<?> conversionClass, SqlFunction downcastFunction, SqlFunction upcastFunction) {
    this(flinkType.getSqlTypeName(), flinkType.getSqlIdentifier(), flinkType.isNullable(), flinkType.getFieldList(),
        flinkType.getComparability(), conversionClass, downcastFunction, upcastFunction);

  }
  public BridgingFlinkType(SqlTypeName typeName, SqlIdentifier sqlIdentifier, boolean nullable, List<? extends RelDataTypeField> fields, RelDataTypeComparability comparability, Class<?> conversionClass, SqlFunction downcastFunction, SqlFunction upcastFunction) {
    super(typeName, sqlIdentifier, nullable, fields, comparability);
    this.conversionClass = conversionClass;
    this.downcastFunction = downcastFunction;
    this.upcastFunction = upcastFunction;
  }
}
