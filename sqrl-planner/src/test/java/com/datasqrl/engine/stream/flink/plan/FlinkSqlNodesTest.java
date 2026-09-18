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
package com.datasqrl.engine.stream.flink.plan;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.SqlConvertersFactory;
import com.datasqrl.engine.stream.flink.FlinkCalciteParser;
import com.datasqrl.engine.stream.flink.FlinkSqlNodes;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlMetadataColumn;
import org.apache.flink.sql.parser.ddl.SqlTableColumn.SqlRegularColumn;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTableLike;
import org.apache.flink.sql.parser.ddl.table.SqlTableLike;
import org.apache.flink.sql.parser.dml.SqlInsertConflictBehavior;
import org.apache.flink.sql.parser.type.SqlRawTypeNameSpec;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.GenericInMemoryCatalog;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class FlinkSqlNodesTest {

  @ParameterizedTest
  @CsvSource(
      delimiter = '|',
      textBlock =
          """
          original_db.SourceTable.val | original_db.`SourceTable` | `other_catalog`.`original_db`.`SourceTable`.`val`
          original_db.SourceTable.*   | original_db.`SourceTable` | `other_catalog`.`original_db`.`SourceTable`.*
          original_db.SourceTable.val | `SourceTable` AS original_db | `original_db`.`SourceTable`.`val`
          """)
  void givenQualifiedColumnOrRowField_whenCatalogChanges_thenRetainsIdentityAndScope(
      String column, String source, String expectedColumn) {
    var env = (TableEnvironmentImpl) TableEnvironment.create(EnvironmentSettings.inBatchMode());
    try {
      var ddl =
          "CREATE TABLE SourceTable (val INT, SourceTable ROW<val INT>) "
              + "WITH ('connector'='datagen','number-of-rows'='1')";
      env.executeSql(ddl);
      env.registerCatalog(
          "other_catalog", new GenericInMemoryCatalog("other_catalog", "original_db"));
      env.useCatalog("other_catalog");
      env.executeSql(ddl);
      var query = FlinkCalciteParser.parseSql("SELECT " + column + " FROM " + source, env);
      var from = ((SqlSelect) query).getFrom();
      var table = (SqlIdentifier) (from instanceof SqlCall alias ? alias.operand(0) : from);
      var position = table.getParserPosition();
      var componentPosition = table.getComponentParserPosition(table.names.size() - 1);
      var planner = ((PlannerBase) env.getPlanner()).createFlinkPlanner();
      var validator = planner.getOrCreateSqlValidator();
      var validated = validator.validate(FlinkSqlNodes.copyQuery(query));
      FlinkSqlNodes.bindTableNames(query, validated, validator);

      assertThat(unparse(query)).contains("SELECT " + expectedColumn);
      assertThat(table.getParserPosition()).isEqualTo(position);
      assertThat(table.getComponentParserPosition(2)).isEqualTo(componentPosition);
      assertThat(table.isComponentQuoted(2)).isTrue();
      env.useCatalog("default_catalog");
      assertThat(env.explainSql(unparse(query)))
          .contains("table=[[other_catalog, original_db, SourceTable]]")
          .doesNotContain("table=[[default_catalog, default_database, SourceTable]]");
    } finally {
      env.getCatalogManager().close();
    }
  }

  private String unparse(SqlNode node) {
    return SqlConvertersFactory.get(Dialect.FLINK).convert(node);
  }

  @Test
  void createView() {
    var tableName = "my_view";
    SqlNode fromTable = FlinkSqlNodes.identifier("source_table");
    var selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    var select =
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            selectList,
            fromTable,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    var createView = FlinkSqlNodes.createView(tableName, select);
    var sql = unparse(createView);
    var expectedSql =
        """
        CREATE VIEW `my_view`
        AS
        SELECT `*`
        FROM `source_table`""";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createInsert() {
    var targetTable = "target_table";
    SqlNode fromTable = FlinkSqlNodes.identifier("source_table");
    var selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    var select =
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            selectList,
            fromTable,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    var insert =
        FlinkSqlNodes.createInsert(
            select, ObjectIdentifier.of("default_catalog", "default_database", targetTable));
    var sql = unparse(insert);
    var expectedSql =
        """
        INSERT INTO `default_catalog`.`default_database`.`target_table`
        SELECT `*`
         FROM `source_table`""";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void givenConflictBehavior_whenCreateInsert_thenIncludesOnConflictClause() {
    var targetTable = ObjectIdentifier.of("default_catalog", "default_database", "target_table");
    SqlNode fromTable = FlinkSqlNodes.identifier("source_table");
    var selectList = new SqlNodeList(SqlParserPos.ZERO);
    selectList.add(new SqlIdentifier("*", SqlParserPos.ZERO));
    var select =
        new SqlSelect(
            SqlParserPos.ZERO,
            null,
            selectList,
            fromTable,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null);

    var insert =
        FlinkSqlNodes.createInsert(
            select, targetTable, Optional.of(SqlInsertConflictBehavior.DEDUPLICATE));
    var sql = unparse(insert);
    var expectedSql =
        """
        INSERT INTO `default_catalog`.`default_database`.`target_table`
        SELECT `*`
         FROM `source_table`
        ON CONFLICT DO DEDUPLICATE""";

    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createFunction() {
    var functionName = "my_udf";
    var className = "com.example.MyUDF";
    var createFunction = FlinkSqlNodes.createFunction(functionName, className, false);

    var sql = unparse(createFunction);
    var expectedSql = "CREATE FUNCTION IF NOT EXISTS `my_udf` AS 'com.example.MyUDF' LANGUAGE JAVA";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createWatermark() {
    var eventTimeColumn = "timestamp_col";
    var eventTimeIdentifier = FlinkSqlNodes.identifier(eventTimeColumn);
    var delay = "5";
    var watermarkStrategy = FlinkSqlNodes.boundedStrategy(eventTimeIdentifier, delay);

    var watermark = FlinkSqlNodes.createWatermark(eventTimeIdentifier, watermarkStrategy);
    var sql = unparse(watermark);
    var expectedSql = "WATERMARK FOR `timestamp_col` AS `timestamp_col` - INTERVAL '5' SECOND";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createSourceWatermark() {
    var watermark = FlinkSqlNodes.createSourceWatermark(FlinkSqlNodes.identifier("event_time_col"));

    var sql = unparse(watermark);

    assertThat(sql.trim()).isEqualTo("WATERMARK FOR `event_time_col` AS `SOURCE_WATERMARK`()");
  }

  @Test
  void boundedStrategy() {
    var watermark = FlinkSqlNodes.identifier("timestamp_col");
    var delay = "5";

    var boundedStrategy = FlinkSqlNodes.boundedStrategy(watermark, delay);
    var sql = unparse(boundedStrategy);
    var expectedSql = "`timestamp_col` - INTERVAL '5' SECOND";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createPrimaryKeyConstraint() {
    List<String> primaryKeyColumns = Arrays.asList("id", "timestamp_col");
    var pkConstraint = FlinkSqlNodes.createPrimaryKeyConstraint(primaryKeyColumns);
    var sql = unparse(pkConstraint);
    var expectedSql = "PRIMARY KEY (`id`, `timestamp_col`) NOT ENFORCED";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createProperties() {
    Map<String, String> options = new HashMap<>();
    options.put("connector", "kafka");
    options.put("topic", "my_topic");
    options.put("format", "json");

    var properties = FlinkSqlNodes.createProperties(options);
    var sql = unparse(properties);
    var expectedSql = "'connector' = 'kafka', 'format' = 'json', 'topic' = 'my_topic'";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createPartitionKeys() {
    List<String> partitionKeys = Arrays.asList("year", "month", "day");
    var partitionKeysNode = FlinkSqlNodes.createPartitionKeys(partitionKeys);
    var sql = unparse(partitionKeysNode);
    var expectedSql = "`year`, `month`, `day`";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void resolveRawJsonTypAliases() {
    var position = new SqlParserPos(3, 20, 3, 32);
    var rawJsonType =
        new SqlDataTypeSpec(new SqlUserDefinedTypeNameSpec("RAW_JSON", position), position)
            .withNullable(false);
    var regularColumn =
        new SqlRegularColumn(
            position,
            FlinkSqlNodes.identifier("payload"),
            FlinkSqlNodes.createStringLiteral("payload comment"),
            rawJsonType,
            null);
    var metadataColumn =
        new SqlMetadataColumn(
            position,
            FlinkSqlNodes.identifier("event_time"),
            FlinkSqlNodes.createStringLiteral("metadata comment"),
            rawJsonType,
            SqlLiteral.createCharString("timestamp", position),
            true);
    var tableLike = new SqlTableLike(position, FlinkSqlNodes.identifier("base_table"), List.of());
    var table =
        new SqlCreateTableLike(
            position,
            FlinkSqlNodes.identifier("source_table"),
            new SqlNodeList(List.of(regularColumn, metadataColumn), position),
            List.of(),
            FlinkSqlNodes.createProperties(Map.of("connector", "kafka")),
            FlinkSqlNodes.NO_DISTRIBUTION,
            SqlNodeList.EMPTY,
            null,
            FlinkSqlNodes.createStringLiteral("table comment"),
            tableLike,
            true,
            true);

    var resolved = FlinkSqlNodes.resolveRawJsonTypAliases(table);

    assertThat(resolved).isInstanceOf(SqlCreateTableLike.class);
    assertThat(((SqlCreateTableLike) resolved).getTableLike()).isSameAs(tableLike);
    assertThat(resolved.getProperties()).isEqualTo(table.getProperties());
    assertThat(resolved.getColumnList().get(0)).isInstanceOf(SqlRegularColumn.class);
    assertThat(resolved.getColumnList().get(1)).isInstanceOf(SqlMetadataColumn.class);

    var resolvedRegularColumn = (SqlRegularColumn) resolved.getColumnList().get(0);
    assertThat(resolvedRegularColumn.getType().getTypeNameSpec())
        .isInstanceOf(SqlRawTypeNameSpec.class);
    assertThat(resolvedRegularColumn.getType().getNullable()).isFalse();
    assertThat(resolvedRegularColumn.getComment()).isEqualTo("payload comment");
    assertThat(resolvedRegularColumn.getType().getParserPosition().getLineNum())
        .isEqualTo(position.getLineNum());
    assertThat(resolvedRegularColumn.getType().getParserPosition().getColumnNum())
        .isEqualTo(position.getColumnNum());
    assertThat(resolvedRegularColumn.getType().getParserPosition().getEndColumnNum())
        .isEqualTo(position.getColumnNum() + unparse(resolvedRegularColumn.getType()).length() - 1);

    var resolvedMetadataColumn = (SqlMetadataColumn) resolved.getColumnList().get(1);
    assertThat(resolvedMetadataColumn.getType().getTypeNameSpec())
        .isInstanceOf(SqlRawTypeNameSpec.class);
    assertThat(resolvedMetadataColumn.getMetadataAlias()).contains("timestamp");
    assertThat(resolvedMetadataColumn.isVirtual()).isTrue();
    assertThat(resolvedMetadataColumn.getComment()).isEqualTo("metadata comment");
  }
}
