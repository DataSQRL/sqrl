package com.datasqrl.engine.database.relational.ddl;

import static org.junit.jupiter.api.Assertions.*;

import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateSnowflakeView;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.dialect.SnowflakeSqlDialect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;
import org.junit.jupiter.api.Test;

class SqlCreateSnowflakeViewTest {

  @Test
  public void testCreateViewWithMultipleOptions() {
    SqlParserPos pos = new SqlParserPos(0, 0);
    SqlIdentifier viewName = new SqlIdentifier("advanced_view", pos);
    SqlNodeList columnList = new SqlNodeList(
        ImmutableNullableList.of(
            new SqlIdentifier("column1", pos),
            new SqlIdentifier("column2", pos)
        ), pos);
    SqlSelect select = new SqlSelectBuilder(SqlParserPos.ZERO).setFrom(new SqlIdentifier("X", SqlParserPos.ZERO))
        .build();

    SqlCharStringLiteral comment = SqlLiteral.createCharString("This is an advanced view with multiple features.", pos);

    SqlCreateSnowflakeView createView = new SqlCreateSnowflakeView(pos, true, true, true, false, null, viewName, columnList, select, comment, true);

    SqlDialect dialect = ExtendedSnowflakeSqlDialect.DEFAULT;
    SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
    createView.unparse(writer, 0, 0);

    String expectedSql = "CREATE OR REPLACE SECURE VIEW IF NOT EXISTS advanced_view(column1, column2)\n"
        + "COPY GRANTS\n"
        + "AS SELECT *\n"
        + "FROM X";
    assertEquals(expectedSql, writer.toString(), "Generated SQL does not match expected.");
  }
}