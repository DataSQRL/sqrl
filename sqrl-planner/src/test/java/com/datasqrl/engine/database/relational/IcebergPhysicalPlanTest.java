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

import com.datasqrl.deployment.model.IcebergPlanModel;
import com.datasqrl.deployment.model.IcebergTableModel;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.database.CombinedEnginePlan;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.SqrlObjectMapper;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class IcebergPhysicalPlanTest {

  private static final IcebergTableModel TABLE =
      new IcebergTableModel("_MyApplications1", "default_database", "hadoop", null, "/tmp/duckdb");

  @Test
  void givenEnginePlansAndTables_whenWritten_thenKeepsThePlansNextToTheTables() throws Exception {
    var plan = new IcebergPhysicalPlan(enginePlans(), List.of(TABLE));

    var json = SqrlObjectMapper.INSTANCE.writeValueAsString(plan);
    var tree = SqrlObjectMapper.INSTANCE.readTree(json);

    assertThat(tree.properties()).extracting(Map.Entry::getKey).containsExactly("plans", "tables");
    assertThat(tree.get("plans").properties())
        .extracting(Map.Entry::getKey)
        .containsExactly("", "duckdb");
    assertThat(tree.get("tables").get(0).has("catalogImpl")).isFalse();
    assertThat(ConfigLoaderUtils.MAPPER.readValue(json, IcebergPlanModel.class).tables())
        .containsExactly(TABLE);
  }

  @Test
  void givenEnginePlans_whenGetDeploymentArtifacts_thenReturnsThoseOfTheCombinedPlan() {
    var plan = new IcebergPhysicalPlan(enginePlans(), List.of(TABLE));

    assertThat(plan.getDeploymentArtifacts())
        .isEqualTo(
            CombinedEnginePlan.builder().plans(enginePlans()).build().getDeploymentArtifacts());
  }

  private static Map<String, EnginePhysicalPlan> enginePlans() {
    var plans = new LinkedHashMap<String, EnginePhysicalPlan>();
    plans.put("", JdbcPhysicalPlan.builder().build());
    plans.put("duckdb", JdbcPhysicalPlan.builder().build());
    return plans;
  }
}
