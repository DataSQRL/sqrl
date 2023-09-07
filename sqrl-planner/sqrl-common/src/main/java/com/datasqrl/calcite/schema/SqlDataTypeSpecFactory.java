package com.datasqrl.calcite.schema;

import java.nio.charset.Charset;
import java.util.Objects;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeUtil;

public class SqlDataTypeSpecFactory {

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
