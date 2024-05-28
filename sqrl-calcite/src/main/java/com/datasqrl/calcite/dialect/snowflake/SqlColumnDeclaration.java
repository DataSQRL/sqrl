package com.datasqrl.calcite.dialect.snowflake;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 * Represents a column declaration in a SQL CREATE TABLE statement.
 */
public class SqlColumnDeclaration extends SqlCall {
    private final SqlIdentifier name;
    private final SqlDataTypeSpec type;
    private final SqlNodeList constraints; // Includes not null, unique, primary key, foreign key
    private final SqlLiteral notNull; // Specific handling for NOT NULL for clarity

    public SqlColumnDeclaration(SqlParserPos pos, SqlIdentifier name, SqlDataTypeSpec type, SqlNodeList constraints, SqlLiteral notNull) {
        super(pos);
        this.name = Objects.requireNonNull(name);
        this.type = Objects.requireNonNull(type);
        this.constraints = constraints; // Can include unique, primary key, foreign key
        this.notNull = notNull;
    }

    @Override
    public SqlOperator getOperator() {
        return new SqlSpecialOperator("COLUMN DECLARATION", SqlKind.OTHER);
    }

    @Override
    public List<SqlNode> getOperandList() {
        // Handling for displaying operand list in debugging and tree operations
        return ImmutableNullableList.of(name, type, constraints, notNull);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        name.unparse(writer, leftPrec, rightPrec);
        type.unparse(writer, leftPrec, rightPrec);
        if (notNull != null) {
            writer.keyword("NOT NULL");
        }
        if (constraints != null) {
            constraints.unparse(writer, leftPrec, rightPrec);
        }
    }
}
