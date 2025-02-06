package com.datasqrl.engine.database.relational.ddl;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.util.ImmutableNullableList;
import org.junit.jupiter.api.Test;

import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.datasqrl.calcite.dialect.snowflake.SqlCreateSnowflakeView;
import com.datasqrl.calcite.schema.sql.SqlBuilders.SqlSelectBuilder;

class SqlCreateSnowflakeViewTest {

  @Test
  public void testCreateViewWithMultipleOptions() {
    var pos = new SqlParserPos(0, 0);
    var viewName = new SqlIdentifier("advanced_view", pos);
    var columnList = new SqlNodeList(
        ImmutableNullableList.of(
            new SqlIdentifier("column1", pos),
            new SqlIdentifier("column2", pos)
        ), pos);
    var select = new SqlSelectBuilder(SqlParserPos.ZERO).setFrom(new SqlIdentifier("X", SqlParserPos.ZERO))
        .build();

    var comment = SqlLiteral.createCharString("This is an advanced view with multiple features.", pos);

    var createView = new SqlCreateSnowflakeView(pos, true, true, true, false, null, viewName, columnList, select, comment, true);

    var dialect = ExtendedSnowflakeSqlDialect.DEFAULT;
    var writer = new SqlPrettyWriter(dialect);
    createView.unparse(writer, 0, 0);

    var expectedSql = """
        CREATE OR REPLACE SECURE VIEW IF NOT EXISTS advanced_view(column1, column2)
        COPY GRANTS
        AS SELECT *
        FROM X""";
    assertEquals(expectedSql, writer.toString(), "Generated SQL does not match expected.");
  }
}