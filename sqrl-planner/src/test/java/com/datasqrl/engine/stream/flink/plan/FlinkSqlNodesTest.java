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
import com.datasqrl.engine.stream.flink.FlinkSqlNodes;
import com.datasqrl.engine.stream.flink.FlinkSqlNodes.MetadataEntry;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.jupiter.api.Test;

class FlinkSqlNodesTest {

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

    var insert = FlinkSqlNodes.createInsert(select, targetTable);
    var sql = unparse(insert);
    var expectedSql =
        """
        INSERT INTO `target_table`
        SELECT `*`
         FROM `source_table`""";
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
}
