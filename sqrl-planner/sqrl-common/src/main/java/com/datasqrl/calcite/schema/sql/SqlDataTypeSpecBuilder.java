package com.datasqrl.calcite.schema.sql;

import lombok.experimental.UtilityClass;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;

@UtilityClass
public class SqlDataTypeSpecBuilder {

  public static SqlDataTypeSpec create(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    SqlBasicTypeNameSpec basicTypeNameSpec;

    if (typeName.allowsPrecScale(true, true)) {
      basicTypeNameSpec = new SqlBasicTypeNameSpec(typeName, type.getPrecision(), type.getScale(), SqlParserPos.ZERO);
    } else if (typeName.allowsPrecScale(true, false)) {
      basicTypeNameSpec = new SqlBasicTypeNameSpec(typeName, type.getPrecision(), SqlParserPos.ZERO);
    } else {
      basicTypeNameSpec = new SqlBasicTypeNameSpec(typeName, SqlParserPos.ZERO);
    }
    return new SqlDataTypeSpec(basicTypeNameSpec, SqlParserPos.ZERO);
  }
}
