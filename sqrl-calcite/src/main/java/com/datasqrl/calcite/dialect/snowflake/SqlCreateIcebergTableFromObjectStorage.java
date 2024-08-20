package com.datasqrl.calcite.dialect.snowflake;

import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.Objects;

/**
 * Iceberg files in object storage
 *
 * CREATE [ OR REPLACE ] ICEBERG TABLE [ IF NOT EXISTS ] <table_name>
 *   [ EXTERNAL_VOLUME = '<external_volume_name>' ]
 *   [ CATALOG = '<catalog_integration_name>' ]
 *   [ METADATA_FILE_PATH = '<metadata_file_path>' ]
 *   [ REPLACE_INVALID_CHARACTERS = { TRUE | FALSE } ]
 *   [ COMMENT = '<string_literal>' ]
 *   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
 */
public class SqlCreateIcebergTableFromObjectStorage extends SqlCall {

  private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE ICEBERG TABLE",
      SqlKind.OTHER_DDL);

  private final SqlIdentifier tableName;
  private final SqlLiteral externalVolume;
  private final SqlLiteral catalog;
  private final SqlLiteral catalogTableName;
  private final SqlLiteral metadataFilePath;
  private final SqlLiteral replaceInvalidCharacters;
  private final SqlLiteral comment;
  private final SqlNodeList tags;
  private final boolean replace;
  private final boolean ifNotExists;

  public SqlCreateIcebergTableFromObjectStorage(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier tableName,
      SqlLiteral externalVolume, SqlLiteral catalog, SqlLiteral catalogTableName, SqlLiteral metadataFilePath,
      SqlLiteral replaceInvalidCharacters, SqlLiteral comment, SqlNodeList tags) {
    super(pos);
    this.replace = replace;
    this.ifNotExists = ifNotExists;
    this.tableName = Objects.requireNonNull(tableName);
    this.externalVolume = externalVolume;
    this.catalog = catalog;
    this.catalogTableName = catalogTableName;
    this.metadataFilePath = metadataFilePath;
    this.replaceInvalidCharacters = replaceInvalidCharacters;
    this.comment = comment;
    this.tags = tags;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableName, externalVolume, catalog, metadataFilePath,
        replaceInvalidCharacters, comment, tags);
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

    if (metadataFilePath != null) {
      writer.keyword("METADATA_FILE_PATH");
      writer.literal("= '" + metadataFilePath.toValue() + "'");
    }

    if (replaceInvalidCharacters != null) {
      writer.keyword("REPLACE_INVALID_CHARACTERS");
      writer.literal("= " + replaceInvalidCharacters.toValue());
    }
    if (comment != null) {
      writer.keyword("COMMENT");
      writer.literal("= '" + comment.toValue() + "'");
    }
    if (tags != null && !tags.getList().isEmpty()) {
      writer.keyword("WITH TAG");
      SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      tags.unparse(writer, 0, 0);
      writer.endList(frame);
    }
  }
}
