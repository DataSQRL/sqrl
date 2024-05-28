package com.datasqrl.calcite.dialect.snowflake;

import java.util.List;
import javax.annotation.Nonnull;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

/**
 * inlineConstraint ::=
 *   [ CONSTRAINT <constraint_name> ]
 *   { UNIQUE
 *     | PRIMARY KEY
 *     | [ FOREIGN KEY ] REFERENCES <ref_table_name> [ ( <ref_col_name> ) ]
 *   }
 *   [ <constraint_properties> ]
 */
public abstract class SqlConstraint extends SqlCall {

  protected final SqlIdentifier constraintName;  // Optional

  protected SqlConstraint(SqlParserPos pos, SqlIdentifier constraintName) {
    super(pos);
    this.constraintName = constraintName;
  }

  @Nonnull
  @Override
  public List<SqlNode> getOperandList() {
    return List.of(constraintName);
  }


  public SqlIdentifier getConstraintName() {
    return constraintName;
  }

  @Override
  public SqlOperator getOperator() {
    return new SqlSpecialOperator("CONSTRAINT", SqlKind.OTHER_DDL);
  }
}
