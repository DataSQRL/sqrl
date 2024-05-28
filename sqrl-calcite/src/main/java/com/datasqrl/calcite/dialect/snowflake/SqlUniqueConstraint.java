package com.datasqrl.calcite.dialect.snowflake;

import java.util.List;
import javax.annotation.Nonnull;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

public class SqlUniqueConstraint extends SqlConstraint {
    public SqlUniqueConstraint(SqlParserPos pos, SqlIdentifier constraintName) {
        super(pos, constraintName);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (constraintName != null) {
            writer.keyword("CONSTRAINT");
            constraintName.unparse(writer, leftPrec, rightPrec);
        }
        writer.keyword("UNIQUE");
    }
}
