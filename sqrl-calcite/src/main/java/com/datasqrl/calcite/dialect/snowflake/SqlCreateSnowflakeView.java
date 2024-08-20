package com.datasqrl.calcite.dialect.snowflake;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Objects;

/**
 * CREATE [ OR REPLACE ] [ SECURE ] [ { [ { LOCAL | GLOBAL } ] TEMP | TEMPORARY | VOLATILE } ] [ RECURSIVE ] VIEW [ IF NOT EXISTS ] <name>
 *   [ ( <column_list> ) ]
 *   [ <col1> [ WITH ] MASKING POLICY <policy_name> [ USING ( <col1> , <cond_col1> , ... ) ]
 *            [ WITH ] PROJECTION POLICY <policy_name>
 *            [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
 *   [ , <col2> [ ... ] ]
 *   [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
 *   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
 *   [ COPY GRANTS ]
 *   [ COMMENT = '<string_literal>' ]
 *   [ [ WITH ] ROW ACCESS POLICY <policy_name> ON ( <col_name> [ , <col_name> ... ] ) ]
 *   [ [ WITH ] AGGREGATION POLICY <policy_name> ]
 *   [ [ WITH ] TAG ( <tag_name> = '<tag_value>' [ , <tag_name> = '<tag_value>' , ... ] ) ]
 *   AS <select_statement>
 */
public class SqlCreateSnowflakeView extends SqlCall {

  private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE VIEW",
      SqlKind.CREATE_VIEW);

  private final SqlIdentifier viewName;
  private final SqlNodeList columnList;
  private final SqlSelect select;
  private final boolean replace;
  private final boolean ifNotExists;
  private final boolean secure;
  private final boolean recursive;
  private final SqlIdentifier tempOption; // Now using SqlIdentifier for symbolic representation
  private final SqlCharStringLiteral comment;
  private final boolean copyGrants;

  public SqlCreateSnowflakeView(SqlParserPos pos, boolean replace, boolean ifNotExists, boolean secure,
      boolean recursive, SqlIdentifier tempOption, SqlIdentifier viewName, SqlNodeList columnList,
      SqlSelect select, SqlCharStringLiteral comment, boolean copyGrants) {
    super(pos);
    this.replace = replace;
    this.ifNotExists = ifNotExists;
    this.secure = secure;
    this.recursive = recursive;
    this.tempOption = tempOption;
    this.viewName = Objects.requireNonNull(viewName);
    this.columnList = columnList;
    this.select = Objects.requireNonNull(select);
    this.comment = comment;
    this.copyGrants = copyGrants;
  }

  @Nonnull
  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Nonnull
  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(viewName, columnList, select, comment, tempOption);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (replace) {
      writer.keyword("OR REPLACE");
    }
    if (secure) {
      writer.keyword("SECURE");
    }
    if (tempOption != null) {
      tempOption.unparse(writer, leftPrec, rightPrec);
    }
    if (recursive) {
      writer.keyword("RECURSIVE");
    }
    writer.keyword("VIEW");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    viewName.unparse(writer, leftPrec, rightPrec);

    if (columnList != null && columnList.size() > 0) {
      SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      columnList.unparse(writer, 0, 0);
      writer.endList(frame);
    }

//    if (comment != null) {
//      writer.newlineAndIndent();
//      writer.keyword("COMMENT");
//      comment.unparse(writer, leftPrec, rightPrec);
//    }

    if (copyGrants) {
      writer.newlineAndIndent();
      writer.keyword("COPY GRANTS");
    }

    writer.newlineAndIndent();
    writer.keyword("AS");
    select.unparse(writer, leftPrec, rightPrec);
  }
}