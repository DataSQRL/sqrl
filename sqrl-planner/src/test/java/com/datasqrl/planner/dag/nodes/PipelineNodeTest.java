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
package com.datasqrl.planner.dag.nodes;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.EngineType;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.EngineStage;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.planner.analyzer.cost.ComputeCost;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class PipelineNodeTest {

  private static ExecutionStage createStage(String stageName, EngineType engineType) {
    var engine = mock(ExecutionEngine.class);
    when(engine.getType()).thenReturn(engineType);
    return new EngineStage(stageName, engine);
  }

  private static Map<ExecutionStage, StageAnalysis> createStageAnalysis(
      List<ExecutionStage> stagesInMapOrder, List<Double> costsInMapOrder) {
    Map<ExecutionStage, StageAnalysis> stageAnalysis = new LinkedHashMap<>();
    for (var index = 0; index < stagesInMapOrder.size(); index++) {
      var stage = stagesInMapOrder.get(index);
      stageAnalysis.put(
          stage,
          new StageAnalysis.Cost(stage, new ComputeCost.Simple(costsInMapOrder.get(index)), true));
    }
    return stageAnalysis;
  }

  @Test
  void givenFlinkAndPostgresWithEqualCost_whenFindCheapestStage_thenPicksFlink() {
    var flinkStage = createStage("flink", EngineType.PROCESS);
    var postgresStage = createStage("postgres", EngineType.DATABASE);

    var postgresFirst = createStageAnalysis(List.of(postgresStage, flinkStage), List.of(1.0, 1.0));
    var flinkFirst = createStageAnalysis(List.of(flinkStage, postgresStage), List.of(1.0, 1.0));

    assertThat(PipelineNode.findCheapestStage(postgresFirst).getStage()).isEqualTo(flinkStage);
    assertThat(PipelineNode.findCheapestStage(flinkFirst).getStage()).isEqualTo(flinkStage);
  }

  @Test
  void givenPostgresCheaperThanFlink_whenFindCheapestStage_thenPicksPostgres() {
    var flinkStage = createStage("flink", EngineType.PROCESS);
    var postgresStage = createStage("postgres", EngineType.DATABASE);

    var stageAnalysis = createStageAnalysis(List.of(flinkStage, postgresStage), List.of(10.0, 1.0));

    assertThat(PipelineNode.findCheapestStage(stageAnalysis).getStage()).isEqualTo(postgresStage);
  }
}
