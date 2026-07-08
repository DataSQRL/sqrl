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
package com.datasqrl.planner;

import static com.datasqrl.planner.FlinkPhysicalPlan.ICEBERG_INFER_SOURCE_PARALLELISM;
import static com.datasqrl.planner.FlinkPhysicalPlan.ICEBERG_USE_V2_SINK;
import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.engine.database.relational.IcebergEngineFactory;
import com.datasqrl.engine.stream.flink.FlinkSqlNodes;
import com.datasqrl.planner.tables.FlinkConnectorConfigWrapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.sql.parser.ddl.table.SqlCreateTable;
import org.junit.jupiter.api.Test;

class FlinkPhysicalPlanTest {

  @Test
  void givenIcebergConnector_whenBuild_thenAppliesIcebergConfigAdjustments() {
    var builder = new FlinkPhysicalPlan.Builder(new Configuration());
    builder.add(createTable(IcebergEngineFactory.ENGINE_NAME));

    var plan = builder.build(Optional.empty());

    assertThat(plan.getConfig().getString(ICEBERG_USE_V2_SINK, null)).isEqualTo("true");
    assertThat(plan.getConfig().getString(ICEBERG_INFER_SOURCE_PARALLELISM, null))
        .isEqualTo("false");
  }

  @Test
  void givenNonIcebergConnector_whenBuild_thenDoesNotApplyIcebergConfigAdjustments() {
    var builder = new FlinkPhysicalPlan.Builder(new Configuration());
    builder.add(createTable("filesystem"));

    var plan = builder.build(Optional.empty());

    assertThat(plan.getConfig().getString(ICEBERG_USE_V2_SINK, null)).isNull();
    assertThat(plan.getConfig().getString(ICEBERG_INFER_SOURCE_PARALLELISM, null)).isNull();
  }

  @Test
  void givenCreateTableWithConnectorAndFormats_whenBuild_thenClassifiesPropertiesSeparately() {
    var table =
        new SqlCreateTable(
            SqlParserPos.ZERO,
            FlinkSqlNodes.identifier("source"),
            SqlNodeList.EMPTY,
            List.of(),
            FlinkSqlNodes.createProperties(
                Map.of(
                    FlinkConnectorConfigWrapper.CONNECTOR_KEY, "kafka",
                    FlinkConnectorConfigWrapper.FORMAT_KEY, "json",
                    FlinkConnectorConfigWrapper.VALUE_FORMAT_KEY, "avro")),
            FlinkSqlNodes.NO_DISTRIBUTION,
            SqlNodeList.EMPTY,
            null,
            null,
            false,
            false);

    var builder = new FlinkPhysicalPlan.Builder(new Configuration());
    builder.add(List.of(table), List.of("CREATE TABLE source"));

    var plan = builder.build(Optional.empty());

    assertThat(plan.getConnectors()).containsExactly("kafka");
    assertThat(plan.getFormats()).containsExactlyInAnyOrder("json", "avro");
  }

  private SqlCreateTable createTable(String connector) {
    var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
    var rowType =
        typeFactory.builder().add("id", typeFactory.createSqlType(SqlTypeName.INTEGER)).build();

    return FlinkSqlNodes.createTable("test_table", rowType, Map.of("connector", connector), false);
  }
}
