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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.engine.database.relational.GenericJdbcStatement;
import com.datasqrl.engine.database.relational.JdbcPhysicalPlan;
import com.datasqrl.engine.database.relational.JdbcPlan;
import com.datasqrl.engine.database.relational.JdbcStatement.Type;
import com.datasqrl.util.SqrlObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Guards the plan file formats: the planner-side plan classes must serialize exactly as their
 * sqrl-planner-model counterparts.
 */
class PlanModelSerializationTest {

  @Test
  void givenJdbcPhysicalPlan_whenSerialized_thenMatchesJdbcPlanModel() throws Exception {
    var statement = new GenericJdbcStatement("t1", Type.TABLE, "CREATE TABLE t1");
    var plan = JdbcPhysicalPlan.builder().statement(statement).tableIdMap(Map.of()).build();

    var json =
        SqrlObjectMapper.INSTANCE.readTree(SqrlObjectMapper.INSTANCE.writeValueAsString(plan));

    assertThat(List.copyOf(json.properties()))
        .extracting(Map.Entry::getKey)
        .containsExactly("statements", "standaloneExtensionStatements");
    assertThat(json.get("statements").get(0).get("sql").asText()).isEqualTo("CREATE TABLE t1");

    var roundTrip = SqrlObjectMapper.INSTANCE.treeToValue(json, JdbcPlan.class);
    assertThat(roundTrip.statements()).hasSize(1);
    assertThat(roundTrip.statements().get(0).getSql()).isEqualTo("CREATE TABLE t1");
  }

  @Test
  void givenFlinkPhysicalPlan_whenSerialized_thenMatchesFlinkPlanModel() throws Exception {
    var plan =
        FlinkPhysicalPlan.builder()
            .flinkSql(List.of("CREATE TABLE x"))
            .connectors(Set.of("kafka"))
            .formats(Set.of("json"))
            .functions(Set.of("CREATE FUNCTION f"))
            .build();

    var json =
        SqrlObjectMapper.INSTANCE.readTree(SqrlObjectMapper.INSTANCE.writeValueAsString(plan));

    assertThat(List.copyOf(json.properties()))
        .extracting(Map.Entry::getKey)
        .containsExactly("flinkSql", "connectors", "formats", "functions");
    assertThat(json.get("flinkSql").get(0).asText()).isEqualTo("CREATE TABLE x");
  }
}
