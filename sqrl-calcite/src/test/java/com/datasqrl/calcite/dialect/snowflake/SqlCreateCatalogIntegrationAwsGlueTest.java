package com.datasqrl.calcite.dialect.snowflake;

import static org.junit.jupiter.api.Assertions.*;

import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.NlsString;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class SqlCreateCatalogIntegrationAwsGlueTest {

  @Test
  public void testUnparse() {
    // Setup
    SqlParserPos pos = new SqlParserPos(0, 0);

    // Create identifiers and literals for the SqlCreateCatalogIntegration
    SqlIdentifier name = new SqlIdentifier("glueCatalogInt", pos);
    SqlLiteral catalogSource = SqlLiteral.createCharString("GLUE", pos);
    SqlLiteral tableFormat = SqlLiteral.createCharString("ICEBERG", pos);
    SqlLiteral enabled = SqlLiteral.createBoolean(true, pos);
    SqlCharStringLiteral comment = SqlLiteral.createCharString("Integration for AWS Glue", SqlParserPos.ZERO);

    // Create catalogParams
    SqlNodeList catalogParams = SqlCatalogParams.createGlueCatalogParams(
        pos,
        "arn:aws:iam::123456789012:role/myGlueRole",
        "123456789012",
        "us-east-2",
        "my.catalogdb"
    );

    // Create SqlCreateCatalogIntegration node
    SqlCreateCatalogIntegrationAwsGlue createCatalogIntegration = new SqlCreateCatalogIntegrationAwsGlue(
        pos, false, false, name, catalogSource, tableFormat, catalogParams, enabled, comment
    );

    // Unparse to SQL
    StringBuilder sb = new StringBuilder();
    SqlWriterConfig config = SqlPrettyWriter.config().withDialect(ExtendedSnowflakeSqlDialect.DEFAULT);
    SqlWriter writer = new SqlPrettyWriter(config);
    createCatalogIntegration.unparse(writer, 0, 0);

    // Expected SQL output
    String expectedSql = "CREATE CATALOG INTEGRATION \"glueCatalogInt\"\n" +
        "CATALOG_SOURCE = 'GLUE'\n" +
        "TABLE_FORMAT = 'ICEBERG'\n" +
        "GLUE_AWS_ROLE_ARN = 'arn:aws:iam::123456789012:role/myGlueRole'\n" +
        "GLUE_CATALOG_ID = '123456789012'\n" +
        "GLUE_REGION = 'us-east-2'\n" +
        "CATALOG_NAMESPACE = 'my.catalogdb'\n" +
        "ENABLED = TRUE\n" +
        "COMMENT = 'Integration for AWS Glue'";

    // Assert
    assertEquals(expectedSql, writer.toString().trim());
  }
}
