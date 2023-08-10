package com.datasqrl.calcite.type;

import org.apache.calcite.rel.type.RelDataTypeComparability;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.ObjectSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.types.UnresolvedDataType;

import java.util.List;

public class BridgingFlinkType extends ObjectSqlType {
  public BridgingFlinkType(SqlTypeName typeName, SqlIdentifier sqlIdentifier, boolean nullable, List<? extends RelDataTypeField> fields, RelDataTypeComparability comparability) {
    super(typeName, sqlIdentifier, nullable, fields, comparability);
  }
}
