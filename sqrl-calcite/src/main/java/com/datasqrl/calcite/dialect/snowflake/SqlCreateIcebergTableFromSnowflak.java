package com.datasqrl.calcite.dialect.snowflake;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import java.util.List;
import java.util.Objects;

/**
 *  Snowflake iceberg table
 *
 * CREATE [ OR REPLACE ] ICEBERG TABLE [ IF NOT EXISTS ] <table_name> (
 *     -- Column definition
 *     <col_name> <col_type>
 *       [ inlineConstraint ]
 *       [ NOT NULL ]
 *       [ COLLATE '<collation_specification>' ]
 *       [ { DEFAULT <expr>
 *           | { AUTOINCREMENT | IDENTITY }
 *             [ { ( <start_num> , <step_num> )
 *                 | START <num> INCREMENT <num>
 *               } ]
 *         } ]
 *       [ [ WITH ] MASKING POLICY <policy_name> [ USING ( <col_name> , <cond_col1> , ... ) ] ]
 *       [ [ WITH ] PROJECTION POLICY <policy_name> ]
 *       [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
 *       [ COMMENT '<string_literal>' ]
 *
 *     -- Additional column definitions
 *     [ , <col_name> <col_type> [ ... ] ]
 *
 *     -- Out-of-line constraints
 *     [ , outoflineConstraint [ ... ] ]
 *   )
 *   [ CLUSTER BY ( <expr> [ , <expr> , ... ] ) ]
 *   [ EXTERNAL_VOLUME = '<external_volume_name>' ]
 *   [ CATALOG = 'SNOWFLAKE' ]
 *   BASE_LOCATION = '<relative_path_from_external_volume>'
 *   [ STORAGE_SERIALIZATION_POLICY = { COMPATIBLE | OPTIMIZED } ]
 *   [ DATA_RETENTION_TIME_IN_DAYS = <integer> ]
 *   [ MAX_DATA_EXTENSION_TIME_IN_DAYS = <integer> ]
 *   [ CHANGE_TRACKING = { TRUE | FALSE } ]
 *   [ DEFAULT_DDL_COLLATION = '<collation_specification>' ]
 *   [ COPY GRANTS ]
 *   [ COMMENT = '<string_literal>' ]
 *   [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
 *   [ [ WITH ] AGGREGATION POLICY <policy_name> ]
 *   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
 */
public class SqlCreateIcebergTableFromSnowflak extends SqlCall {

  private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE ICEBERG TABLE",
      SqlKind.OTHER_DDL);

  private final SqlIdentifier tableName;
  private final List<SqlColumnDeclaration> columns;
  private final SqlNodeList tableOptions;
  private final boolean replace;
  private final boolean ifNotExists;
  private final SqlNodeList columnConstraints;
  private final SqlLiteral comment;
  private final SqlNodeList clusterBy;
  private final SqlLiteral externalVolume;
  private final SqlLiteral catalog;
  private final SqlLiteral baseLocation;
  private final SqlLiteral storageSerializationPolicy;
  private final SqlLiteral dataRetentionTimeInDays;
  private final SqlLiteral maxDataExtensionTimeInDays;
  private final SqlLiteral changeTracking;
  private final SqlLiteral defaultDdlCollation;
  private final boolean copyGrants;

  public SqlCreateIcebergTableFromSnowflak(SqlParserPos pos, boolean replace, boolean ifNotExists,
      SqlIdentifier tableName,
      List<SqlColumnDeclaration> columns, SqlNodeList columnConstraints,
      SqlLiteral comment, SqlNodeList tableOptions, SqlNodeList clusterBy,
      SqlLiteral externalVolume,
      SqlLiteral catalog, SqlLiteral baseLocation, SqlLiteral storageSerializationPolicy,
      SqlLiteral dataRetentionTimeInDays, SqlLiteral maxDataExtensionTimeInDays,
      SqlLiteral changeTracking, SqlLiteral defaultDdlCollation, boolean copyGrants) {
    super(pos);
    this.replace = replace;
    this.ifNotExists = ifNotExists;
    this.tableName = Objects.requireNonNull(tableName);
    this.columns = Objects.requireNonNull(columns);
    this.columnConstraints = columnConstraints;
    this.comment = comment;
    this.tableOptions = tableOptions;
    this.clusterBy = clusterBy;
    this.externalVolume = externalVolume;
    this.catalog = catalog;
    this.baseLocation = baseLocation;
    this.storageSerializationPolicy = storageSerializationPolicy;
    this.dataRetentionTimeInDays = dataRetentionTimeInDays;
    this.maxDataExtensionTimeInDays = maxDataExtensionTimeInDays;
    this.changeTracking = changeTracking;
    this.defaultDdlCollation = defaultDdlCollation;
    this.copyGrants = copyGrants;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(tableName);
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

    // Column definitions
    SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
    for (SqlColumnDeclaration column : columns) {
      writer.sep(",");
      column.unparse(writer, leftPrec, rightPrec);
    }
    if (columnConstraints != null) {
      columnConstraints.unparse(writer, 0, 0);
    }
    writer.endList(frame);

    // Table options like CLUSTER BY, EXTERNAL_VOLUME
    if (clusterBy != null) {
      writer.keyword("CLUSTER BY");
      clusterBy.unparse(writer, leftPrec, rightPrec);
    }
    if (externalVolume != null) {
      writer.keyword("EXTERNAL_VOLUME");
      writer.literal("= '" + externalVolume.toValue() + "'");
    }
    if (catalog != null) {
      writer.keyword("CATALOG");
      writer.literal("= 'SNOWFLAKE'");
    }
    writer.keyword("BASE_LOCATION");
    writer.literal("= '" + baseLocation.toValue() + "'");
    if (storageSerializationPolicy != null) {
      writer.keyword("STORAGE_SERIALIZATION_POLICY");
      storageSerializationPolicy.unparse(writer, leftPrec, rightPrec);
    }
    if (dataRetentionTimeInDays != null) {
      writer.keyword("DATA_RETENTION_TIME_IN_DAYS");
      dataRetentionTimeInDays.unparse(writer, leftPrec, rightPrec);
    }
    if (maxDataExtensionTimeInDays != null) {
      writer.keyword("MAX_DATA_EXTENSION_TIME_IN_DAYS");
      maxDataExtensionTimeInDays.unparse(writer, leftPrec, rightPrec);
    }
    if (changeTracking != null) {
      writer.keyword("CHANGE_TRACKING");
      changeTracking.unparse(writer, leftPrec, rightPrec);
    }
    if (defaultDdlCollation != null) {
      writer.keyword("DEFAULT_DDL_COLLATION");
      defaultDdlCollation.unparse(writer, leftPrec, rightPrec);
    }
    if (copyGrants) {
      writer.keyword("COPY GRANTS");
    }

    // Comment
    if (comment != null) {
      writer.newlineAndIndent();
      writer.keyword("COMMENT");
      comment.unparse(writer, leftPrec, rightPrec);
    }

  }
}
