package com.datasqrl.calcite.dialect.snowflake;

import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.Objects;

/**
 * For AWS Glue
 * CREATE [ OR REPLACE ] ICEBERG TABLE [ IF NOT EXISTS ] <table_name>
 *   [ EXTERNAL_VOLUME = '<external_volume_name>' ]
 *   [ CATALOG = '<catalog_integration_name>' ]
 *   CATALOG_TABLE_NAME = '<catalog_table_name>'
 *   [ CATALOG_NAMESPACE = '<catalog_namespace>' ]
 *   [ REPLACE_INVALID_CHARACTERS = { TRUE | FALSE } ]
 *   [ COMMENT = '<string_literal>' ]
 *   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
 */
public class SqlCreateIcebergTableFromAWSGlue extends SqlCall {

  public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE ICEBERG TABLE",
      SqlKind.OTHER_DDL);

  private final SqlIdentifier tableName;
  private final SqlLiteral externalVolume;
  private final SqlLiteral catalog;
  private final SqlLiteral catalogTableName;
  private final SqlLiteral catalogNamespace;
  private final SqlLiteral replaceInvalidCharacters;
  private final SqlLiteral comment;
  private final boolean replace;
  private final boolean ifNotExists;

  public SqlCreateIcebergTableFromAWSGlue(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier tableName,
      SqlLiteral externalVolume, SqlLiteral catalog, SqlLiteral catalogTableName,
      SqlLiteral catalogNamespace, SqlLiteral replaceInvalidCharacters, SqlLiteral comment) {
    super(pos);
    this.replace = replace;
    this.ifNotExists = ifNotExists;
    this.tableName = Objects.requireNonNull(tableName);
    this.externalVolume = externalVolume;
    this.catalog = catalog;
    this.catalogTableName = Objects.requireNonNull(catalogTableName);
    this.catalogNamespace = catalogNamespace;
    this.replaceInvalidCharacters = replaceInvalidCharacters;
    this.comment = comment;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableName, externalVolume, catalog, catalogTableName,
        catalogNamespace,
        replaceInvalidCharacters, comment);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (replace) {
      writer.keyword("OR REPLACE");
    }
    writer.keyword("ICEBERG TABLE");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    tableName.unparse(writer, leftPrec, rightPrec);

    if (externalVolume != null) {
      writer.keyword("EXTERNAL_VOLUME");
      writer.literal("= '" + externalVolume.toValue() + "'");
    }
    if (catalog != null) {
      writer.keyword("CATALOG");
      writer.literal("= '" + catalog.toValue() + "'");
    }

    writer.keyword("CATALOG_TABLE_NAME");
    writer.literal("= '" + catalogTableName.toValue() + "'");

    if (catalogNamespace != null) {
      writer.keyword("CATALOG_NAMESPACE");
      writer.literal("= '" + catalogNamespace.toValue() + "'");
    }
    if (replaceInvalidCharacters != null) {
      writer.keyword("REPLACE_INVALID_CHARACTERS");
      writer.literal("= " + replaceInvalidCharacters.toValue());
    }
    if (comment != null) {
      writer.keyword("COMMENT");
      writer.literal("= '" + comment.toValue() + "'");
    }
  }
}
