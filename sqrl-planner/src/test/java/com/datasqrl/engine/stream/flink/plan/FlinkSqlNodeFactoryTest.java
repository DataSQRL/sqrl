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
import com.datasqrl.calcite.convert.SqlToStringFactory;
import com.datasqrl.engine.stream.flink.plan.FlinkSqlNodeFactory.MetadataEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;

class FlinkSqlNodeFactoryTest {

  public record MockMetadataEntry(
      Optional<String> type, Optional<String> attribute, Optional<Boolean> virtual)
      implements MetadataEntry {}

  private String unparse(SqlNode node) {
    var sqlToString = SqlToStringFactory.get(Dialect.FLINK);
    return sqlToString.convert(() -> node).getSql();
  }

  @Test
  void createView() {
    var tableName = "my_view";
    SqlNode fromTable = FlinkSqlNodeFactory.identifier("source_table");
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
            null);

    var createView = FlinkSqlNodeFactory.createView(tableName, select);
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
    SqlNode fromTable = FlinkSqlNodeFactory.identifier("source_table");
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
            null);

    var insert = FlinkSqlNodeFactory.createInsert(select, targetTable);
    var sql = unparse(insert);
    var expectedSql =
        """
        INSERT INTO `target_table`
        (SELECT `*`
         FROM `source_table`)""";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createFunction() {
    var functionName = "my_udf";
    var className = "com.example.MyUDF";
    var createFunction = FlinkSqlNodeFactory.createFunction(functionName, className, false);

    var sql = unparse(createFunction);
    var expectedSql = "CREATE FUNCTION IF NOT EXISTS `my_udf` AS 'com.example.MyUDF' LANGUAGE JAVA";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createWatermark() {
    var eventTimeColumn = "timestamp_col";
    var eventTimeIdentifier = FlinkSqlNodeFactory.identifier(eventTimeColumn);
    var delay = "5";
    var watermarkStrategy = FlinkSqlNodeFactory.boundedStrategy(eventTimeIdentifier, delay);

    var watermark = FlinkSqlNodeFactory.createWatermark(eventTimeIdentifier, watermarkStrategy);
    var sql = unparse(watermark);
    var expectedSql = "WATERMARK FOR `timestamp_col` AS `timestamp_col` - INTERVAL '5' SECOND";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void boundedStrategy() {
    var watermark = FlinkSqlNodeFactory.identifier("timestamp_col");
    var delay = "5";

    var boundedStrategy = FlinkSqlNodeFactory.boundedStrategy(watermark, delay);
    var sql = unparse(boundedStrategy);
    var expectedSql = "`timestamp_col` - INTERVAL '5' SECOND";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createPrimaryKeyConstraint() {
    List<String> primaryKeyColumns = Arrays.asList("id", "timestamp_col");
    var pkConstraint = FlinkSqlNodeFactory.createPrimaryKeyConstraint(primaryKeyColumns);
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

    var properties = FlinkSqlNodeFactory.createProperties(options);
    var sql = unparse(properties);
    var expectedSql = "'connector' = 'kafka', 'format' = 'json', 'topic' = 'my_topic'";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createPartitionKeys() {
    List<String> partitionKeys = Arrays.asList("year", "month", "day");
    var partitionKeysNode = FlinkSqlNodeFactory.createPartitionKeys(partitionKeys);
    var sql = unparse(partitionKeysNode);
    var expectedSql = "`year`, `month`, `day`";
    assertThat(sql.trim()).isEqualTo(expectedSql);
  }

  @Test
  void createTable() {
    var tableName = "my_table";
    RelDataTypeFactory typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

    // Define the schema
    var intType = typeFactory.createSqlType(SqlTypeName.INTEGER);
    var varcharType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 255);
    var timestampType = typeFactory.createSqlType(SqlTypeName.TIMESTAMP);

    var rowType =
        typeFactory
            .builder()
            .add("id", intType)
            .add("name", varcharType)
            .add("timestamp_col", timestampType)
            .add("metadata_col1", varcharType)
            .add("metadata_col2", varcharType)
            .build();

    Optional<List<String>> partitionKeys = Optional.of(Arrays.asList("name"));
    var watermarkMillis = 5000L;
    Optional<String> timestampColumn = Optional.of("timestamp_col");
    Map<String, MetadataEntry> metadataConfig = new HashMap<>();

    // MetadataEntry with type only
    MetadataEntry metadataEntry1 =
        new MockMetadataEntry(Optional.of("timestamp_col"), Optional.empty(), Optional.empty());
    metadataConfig.put("metadata_col1", metadataEntry1);

    // MetadataEntry with attribute
    MetadataEntry metadataEntry2 =
        new MockMetadataEntry(
            Optional.of("timestamp_col"), Optional.of("timestamp"), Optional.of(true));
    metadataConfig.put("metadata_col2", metadataEntry2);

    List<String> primaryKeyConstraint = Arrays.asList("id");
    Map<String, String> connectorProperties = new LinkedHashMap<>();
    connectorProperties.put("connector", "filesystem");
    connectorProperties.put("path", "/tmp/data");
    connectorProperties.put("format", "csv");

    // Simple MetadataExpressionParser implementation
    FlinkSqlNodeFactory.MetadataExpressionParser expressionParser =
        expression -> {
          // Return an identifier for simplicity
          return FlinkSqlNodeFactory.identifier(expression);
        };

    var createTable =
        FlinkSqlNodeFactory.createTable(
            tableName,
            rowType,
            partitionKeys,
            watermarkMillis,
            timestampColumn,
            metadataConfig,
            primaryKeyConstraint,
            connectorProperties,
            expressionParser);
    // Unparse and compare SQL strings
    var sql = unparse(createTable);
    var expectedSql =
        """
        CREATE TABLE `my_table` (
          `id` INTEGER NOT NULL,
          `name` VARCHAR(255) CHARACTER SET `UTF-16LE` NOT NULL,
          `timestamp_col` TIMESTAMP(0) NOT NULL,
          `metadata_col1` VARCHAR(255) CHARACTER SET `UTF-16LE` NOT NULL METADATA FROM 'timestamp_col',
          `metadata_col2` VARCHAR(255) CHARACTER SET `UTF-16LE` NOT NULL METADATA FROM 'timestamp' VIRTUAL,
          PRIMARY KEY (`id`) NOT ENFORCED,
          WATERMARK FOR `timestamp_col` AS `timestamp_col` - INTERVAL '5.0' SECOND
        )
        PARTITIONED BY (`name`)
        WITH (
          'connector' = 'filesystem',
          'format' = 'csv',
          'path' = '/tmp/data'
        )""";
    assertThat(sql.trim()).isEqualTo(expectedSql.trim());
  }
}
