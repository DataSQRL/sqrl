package com.datasqrl.calcite.type;

import com.datasqrl.calcite.Dialect;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.List;
import java.util.Map;

@Getter
public class BridgingFlinkType extends ObjectSqlType implements DelegatingDataType, PrimitiveTypeAlias {
  private final Class<?> conversionClass;
  private final SqlFunction downcastFunction;
  private final SqlFunction upcastFunction;
  private final Map<Dialect, String> physicalTypeName;

  public BridgingFlinkType(RelDataType flinkType, Class<?> conversionClass, SqlFunction downcastFunction, SqlFunction upcastFunction, Map<Dialect, String> physicalTypeName) {
    this(flinkType.getSqlTypeName(), flinkType.getSqlIdentifier(), flinkType.isNullable(), flinkType.getFieldList(),
        flinkType.getComparability(), conversionClass, downcastFunction, upcastFunction, physicalTypeName);

  }
  public BridgingFlinkType(SqlTypeName typeName, SqlIdentifier sqlIdentifier, boolean nullable, List<? extends RelDataTypeField> fields, RelDataTypeComparability comparability, Class<?> conversionClass, SqlFunction downcastFunction, SqlFunction upcastFunction, Map<Dialect, String> physicalTypeName) {
    super(typeName, sqlIdentifier, nullable, fields, comparability);
    this.conversionClass = conversionClass;
    this.downcastFunction = downcastFunction;
    this.upcastFunction = upcastFunction;
    this.physicalTypeName = physicalTypeName;
  }

  @Override
  public Object getPhysicalType(Dialect dialect) {
    return physicalTypeName.get(dialect);
  }
}
