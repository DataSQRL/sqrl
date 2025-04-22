package com.datasqrl.calcite.dialect.snowflake;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.junit.jupiter.api.Test;

import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;

/**
 * Test class for SqlCreateIcebergTableFromAWSGlue.
 */
public class SqlCreateIcebergTableFromAwsGlueTest {

  @Test
  public void testBasicCreateTable() {
    var tableName = new SqlIdentifier("test_table", SqlParserPos.ZERO);
    SqlLiteral catalogTableName = SqlLiteral.createCharString("test_catalog_table", SqlParserPos.ZERO);
    var createTable = new SqlCreateIcebergTableFromAwsGlue(
        SqlParserPos.ZERO, false, false, tableName, null, null, catalogTableName, null, null, null);

    assertEquals("CREATE ICEBERG TABLE test_table CATALOG_TABLE_NAME = 'test_catalog_table'",
        unparse(createTable));
  }

  @Test
  public void testCreateTableWithAllOptions() {
    var tableName = new SqlIdentifier("test_table", SqlParserPos.ZERO);
    SqlLiteral externalVolume = SqlLiteral.createCharString("vol1", SqlParserPos.ZERO);
    SqlLiteral catalog = SqlLiteral.createCharString("aws_catalog", SqlParserPos.ZERO);
    SqlLiteral catalogTableName = SqlLiteral.createCharString("test_catalog_table", SqlParserPos.ZERO);
    SqlLiteral catalogNamespace = SqlLiteral.createCharString("namespace1", SqlParserPos.ZERO);
    var replaceInvalidCharacters = SqlLiteral.createBoolean(true, SqlParserPos.ZERO);
    SqlLiteral comment = SqlLiteral.createCharString("This is a test table", SqlParserPos.ZERO);

    var createTable = new SqlCreateIcebergTableFromAwsGlue(
        SqlParserPos.ZERO, true, true, tableName, externalVolume, catalog, catalogTableName,
        catalogNamespace, replaceInvalidCharacters, comment);

    assertEquals("CREATE OR REPLACE ICEBERG TABLE IF NOT EXISTS test_table EXTERNAL_VOLUME = 'vol1' CATALOG = 'aws_catalog' CATALOG_TABLE_NAME = 'test_catalog_table' CATALOG_NAMESPACE = 'namespace1' REPLACE_INVALID_CHARACTERS = true COMMENT = 'This is a test table'",
        unparse(createTable));
  }

  private String unparse(SqlNode sqlNode) {
    // Setup for testing SQL string output, using default SqlWriter configuration
    var config = SqlPrettyWriter.config()
        .withDialect(ExtendedSnowflakeSqlDialect.DEFAULT);
    var prettyWriter = new SqlPrettyWriter(config);
    sqlNode.unparse(prettyWriter, 0, 0);
    return prettyWriter.toSqlString().getSql();
  }
}
