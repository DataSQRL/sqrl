package com.datasqrl.calcite.dialect.postgres;

import java.util.List;
import java.util.Objects;

import javax.annotation.Nonnull;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

/**
 * CREATE [ OR REPLACE ] [ TEMP | TEMPORARY ] [ RECURSIVE ] VIEW name
 * [ ( column_name [, ...] ) ]
 * [ WITH ( view_option_name [= view_option_value] [, ... ] ) ]
 * AS query
 * [ WITH [ CASCADED | LOCAL ] CHECK OPTION ]
 */
public class SqlCreatePostgresView extends SqlCall {

  private static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE VIEW", SqlKind.CREATE_VIEW);

  private final boolean replace;
  private final SqlIdentifier viewName;
  private final SqlNodeList columnList;
  private final SqlNode select;

  public SqlCreatePostgresView(SqlParserPos pos, boolean replace,
      SqlIdentifier viewName, SqlNodeList columnList,
      SqlNode select) {
    super(pos);
    this.replace = replace;
    this.viewName = Objects.requireNonNull(viewName);
    this.columnList = columnList;
    this.select = Objects.requireNonNull(select);
  }

  @Nonnull
  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Nonnull
  @Override
  public List<SqlNode> getOperandList() {
    return ImmutableNullableList.of(viewName, columnList, select);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE");
    if (replace) {
      writer.keyword("OR REPLACE");
    }

    writer.keyword("VIEW");
    viewName.unparse(writer, leftPrec, rightPrec);

    if (columnList != null && columnList.size() > 0) {
      var frame = writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
      columnList.unparse(writer, leftPrec, rightPrec);
      writer.endList(frame);
    }

    writer.keyword("AS");
    select.unparse(writer, leftPrec, rightPrec);
  }
}
