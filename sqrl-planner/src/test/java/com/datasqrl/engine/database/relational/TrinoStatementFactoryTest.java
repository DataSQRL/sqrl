/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.engine.database.relational;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SqlConvertersFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.deployment.model.JdbcStatementModel.Field;
import com.datasqrl.deployment.model.JdbcStatementModel.PartitionType;
import com.datasqrl.engine.database.relational.ddl.TrinoCreateTableDdlFactory;
import com.datasqrl.engine.database.relational.ddl.TrinoTypeFormatter;
import java.time.Duration;
import java.util.List;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;
import org.junit.jupiter.api.Test;

class TrinoStatementFactoryTest {
  @Test
  void rendersTrinoFunctionsAndPagination() throws Exception {
    var sql =
        SqlParser.create(
                "SELECT SUBSTRING(\"name\" FROM 2 FOR 3), CHAR_LENGTH(\"name\"), "
                    + "APPROX_COUNT_DISTINCT(\"id\") FROM \"source\" OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY")
            .parseQuery();
    assertThat(SqlConvertersFactory.get(Dialect.TRINO).convert(sql))
        .contains(
            "SUBSTRING(\"name\", 2, 3)",
            "APPROX_DISTINCT(\"id\")",
            "OFFSET 5 ROWS",
            "FETCH NEXT 10 ROWS ONLY");
  }

  @Test
  void preservesLogicalParameterIndexesAndQuotedText() throws Exception {
    var query =
        (SqlSelect)
            SqlParser.create("SELECT 'keep ? and $2', ? + ? + ? FROM \"source\"").parseQuery();
    query
        .getSelectList()
        .set(
            1,
            SqlStdOperatorTable.PLUS.createCall(
                SqlParserPos.ZERO,
                new SqlDynamicParam(2, SqlParserPos.ZERO),
                SqlStdOperatorTable.PLUS.createCall(
                    SqlParserPos.ZERO,
                    new SqlDynamicParam(0, SqlParserPos.ZERO),
                    new SqlDynamicParam(2, SqlParserPos.ZERO))));
    assertThat(SqlConvertersFactory.get(Dialect.TRINO).convert(query))
        .contains("'keep ? and $2'", "$3 + ($1 + $3)");
  }

  @Test
  void mapsNestedTypesAndCreatesView() throws Exception {
    var types = new JavaTypeFactoryImpl();
    var row =
        types
            .builder()
            .add("items", types.createArrayType(types.createSqlType(SqlTypeName.INTEGER), -1))
            .add(
                "attributes",
                types.createMapType(
                    types.createSqlType(SqlTypeName.VARCHAR),
                    types.createSqlType(SqlTypeName.BIGINT)))
            .build();
    var factory = new TrinoStatementFactory(mock(EngineConfig.class));
    assertThat(TrinoTypeFormatter.format(row))
        .isEqualTo("ROW(\"items\" ARRAY(INTEGER), \"attributes\" MAP(VARCHAR, BIGINT))");
    var query =
        SqlParser.create("SELECT CAST(NULL AS INTEGER ARRAY) AS \"items\" FROM \"source\"")
            .parseQuery();
    assertThat(
            factory.createView(
                new SqlIdentifier("my view", SqlParserPos.ZERO),
                new SqlNodeList(
                    List.of(new SqlIdentifier("items", SqlParserPos.ZERO)), SqlParserPos.ZERO),
                query))
        .contains(
            "CREATE OR REPLACE VIEW \"my view\" AS SELECT CAST(NULL AS INTEGER ARRAY) AS \"items\"");
    assertThatThrownBy(() -> factory.addIndex(null))
        .isInstanceOf(UnsupportedOperationException.class);
  }

  @Test
  void createsTablesWithoutPrimaryKeyConstraintsAndPreservesMetadata() {
    var statement =
        new CreateTableJdbcStatement(
            "My Table",
            null,
            List.of(new Field("id", "BIGINT", false, null)),
            List.of("id"),
            List.of(),
            PartitionType.NONE,
            0,
            Duration.ZERO,
            null);
    assertThat(statement.getSql(new TrinoCreateTableDdlFactory()))
        .isEqualTo("CREATE TABLE IF NOT EXISTS \"My Table\" (\"id\" BIGINT NOT NULL)");
    assertThat(statement.getPrimaryKey()).containsExactly("id");
    assertThat(
            new TrinoStatementFactory(mock(EngineConfig.class))
                .getCreateViewDdlFactory()
                .createView("My View", List.of("id"), "SELECT id FROM t"))
        .isEqualTo("CREATE OR REPLACE VIEW \"My View\" AS SELECT id FROM t");
  }

  @Test
  void preservesDecimalAndStringPrecision() throws Exception {
    var types = new FlinkTypeFactory(getClass().getClassLoader(), FlinkTypeSystem.INSTANCE);
    assertThat(TrinoTypeFormatter.format(types.createSqlType(SqlTypeName.DECIMAL, 38, 18)))
        .isEqualTo("DECIMAL(38, 18)");
    assertThat(
            TrinoTypeFormatter.format(types.createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE)))
        .isEqualTo("VARCHAR");
  }

  @Test
  void usesTheCalciteBundledWithFlink() throws Exception {
    var original = org.apache.calcite.sql.SqlDialect.class;
    var source = original.getProtectionDomain().getCodeSource().getLocation();
    assertThat(source.toString()).contains("flink-table-planner");
    assertThatThrownBy(() -> Class.forName("org.apache.calcite.sql.dialect.TrinoSqlDialect"))
        .isInstanceOf(ClassNotFoundException.class);
    assertThat(FlinkTypeFactory.class.getClassLoader().loadClass(original.getName()))
        .isSameAs(original);
  }
}
