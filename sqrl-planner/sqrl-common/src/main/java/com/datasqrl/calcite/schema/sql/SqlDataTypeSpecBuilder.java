package com.datasqrl.calcite.schema.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlDataTypeSpecBuilder {

  public static SqlDataTypeSpec create(RelDataType type) {

    if (type.getSqlTypeName().allowsPrecScale(true, true)) {

      return new SqlDataTypeSpec(new SqlBasicTypeNameSpec(type.getSqlTypeName(),
          type.getPrecision(), type.getScale(), SqlParserPos.ZERO)
          , SqlParserPos.ZERO);
    } else if (type.getSqlTypeName().allowsPrecScale(true, false)) {

      return new SqlDataTypeSpec(new SqlBasicTypeNameSpec(type.getSqlTypeName(), type.getPrecision(), SqlParserPos.ZERO)
          , SqlParserPos.ZERO);
    } else {
      return new SqlDataTypeSpec(new SqlBasicTypeNameSpec(type.getSqlTypeName(), SqlParserPos.ZERO)
          , SqlParserPos.ZERO);
    }
  }
}
