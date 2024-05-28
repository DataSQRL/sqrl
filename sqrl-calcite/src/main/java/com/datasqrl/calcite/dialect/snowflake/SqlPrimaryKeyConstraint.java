package com.datasqrl.calcite.dialect.snowflake;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlPrimaryKeyConstraint extends SqlConstraint {
    public SqlPrimaryKeyConstraint(SqlParserPos pos, SqlIdentifier constraintName) {
        super(pos, constraintName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (constraintName != null) {
            writer.keyword("CONSTRAINT");
            constraintName.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("PRIMARY KEY");
    }
}
