package com.datasqrl.calcite.dialect.snowflake;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Represents a SQL CREATE TABLE command with the ability to use a template.
 */
public class SqlTableUsing extends SqlCall {
    private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE TABLE", SqlKind.CREATE_TABLE);

    private final SqlIdentifier tableName;
    private final SqlNode query;
    private final boolean replace;
    private final boolean copyGrants;

    public SqlTableUsing(SqlParserPos pos, boolean replace, boolean copyGrants, SqlIdentifier tableName, SqlNode query) {
        super(pos);
        this.replace = replace;
        this.copyGrants = copyGrants;
        this.tableName = Objects.requireNonNull(tableName);
        this.query = Objects.requireNonNull(query);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(tableName, query);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (replace) {
            writer.keyword("OR REPLACE");
        }
        writer.keyword("TABLE");
        tableName.unparse(writer, leftPrec, rightPrec);
        
        if (copyGrants) {
            writer.keyword("COPY GRANTS");
        }

        writer.newlineAndIndent();
        writer.keyword("USING TEMPLATE");
        query.unparse(writer, leftPrec, rightPrec);
    }
}