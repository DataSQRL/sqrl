package com.datasqrl.calcite.dialect.snowflake;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * CREATE [ OR REPLACE ] CATALOG INTEGRATION [IF NOT EXISTS]
 *   <name>
 *   CATALOG_SOURCE = { GLUE | OBJECT_STORE }
 *   TABLE_FORMAT = { ICEBERG }
 *   [ catalogParams ]
 *   ENABLED = { TRUE | FALSE }
 *   [ COMMENT = '{string_literal}' ]
 * Where:
 *
 * catalogParams (for AWS Glue)::=
 *   GLUE_AWS_ROLE_ARN = '<arn-for-AWS-role-to-assume>'
 *   GLUE_CATALOG_ID = '<glue-catalog-id>'
 *   [ GLUE_REGION = '<AWS-region-of-the-glue-catalog>' ]
 *   CATALOG_NAMESPACE = '<catalog-namespace>'
 */
public class SqlCreateCatalogIntegrationAwsGlue extends SqlCall {
    public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE CATALOG INTEGRATION", SqlKind.OTHER_DDL);

    private final boolean replace;
    private final boolean ifNotExists;
    private final SqlIdentifier name;
    private final SqlLiteral catalogSource;
    private final SqlLiteral tableFormat;
    private final SqlNodeList catalogParams; // Specific for AWS Glue or other catalog types (list of SqlAssignment)
    private final SqlLiteral enabled;
    private final SqlCharStringLiteral comment;

    public SqlCreateCatalogIntegrationAwsGlue(SqlParserPos pos, boolean replace, boolean ifNotExists, SqlIdentifier name,
                                       SqlLiteral catalogSource, SqlLiteral tableFormat, SqlNodeList catalogParams,
                                       SqlLiteral enabled, SqlCharStringLiteral comment) {
        super(pos);
        this.replace = replace;
        this.ifNotExists = ifNotExists;
        this.name = Objects.requireNonNull(name, "Integration name is required");
        this.catalogSource = Objects.requireNonNull(catalogSource, "Catalog source is required");
        this.tableFormat = Objects.requireNonNull(tableFormat, "Table format is required");
        this.catalogParams = catalogParams; // Can be null if no specific parameters are provided
        this.enabled = Objects.requireNonNull(enabled, "Enabled flag is required");
        this.comment = comment; // Optional
    }

    @Nonnull
    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Nonnull
    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableNullableList.of(name, catalogSource, tableFormat, catalogParams, enabled, comment);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");
        if (replace) {
            writer.keyword("OR REPLACE");
        }
        writer.keyword("CATALOG INTEGRATION");
        if (ifNotExists) {
            writer.keyword("IF NOT EXISTS");
        }
        name.unparse(writer, leftPrec, rightPrec);

        writer.newlineAndIndent();
        writer.keyword("CATALOG_SOURCE =");
        catalogSource.unparse(writer, leftPrec, rightPrec);

        writer.newlineAndIndent();
        writer.keyword("TABLE_FORMAT =");
        tableFormat.unparse(writer, leftPrec, rightPrec);

        if (catalogParams != null && !catalogParams.getList().isEmpty()) {
            for (SqlNode param : catalogParams) {
                writer.newlineAndIndent();
                param.unparse(writer, leftPrec, rightPrec);
            }
        }

        writer.newlineAndIndent();
        writer.keyword("ENABLED =");
        enabled.unparse(writer, leftPrec, rightPrec);

        if (comment != null) {
            writer.newlineAndIndent();
            writer.keyword("COMMENT =");
            comment.unparse(writer, leftPrec, rightPrec);
        }
    }
}
