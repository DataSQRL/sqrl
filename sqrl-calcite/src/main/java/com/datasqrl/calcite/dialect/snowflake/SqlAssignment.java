package com.datasqrl.calcite.dialect.snowflake;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;

public class SqlAssignment extends SqlCall {
    private final SqlLiteral key;
    private final SqlNode value;

    public SqlAssignment(SqlLiteral key, SqlNode value, SqlParserPos pos) {
        super(pos);
        this.key = key;
        this.value = value;
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return new SqlSpecialOperator("ASSIGNMENT", SqlKind.OTHER);
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(key, value);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        key.unparse(writer, leftPrec, rightPrec);
        writer.keyword("=");
        value.unparse(writer, leftPrec, rightPrec);
    }
}
