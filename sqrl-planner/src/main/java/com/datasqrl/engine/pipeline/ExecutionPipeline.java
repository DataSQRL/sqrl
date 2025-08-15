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
package com.datasqrl.engine.pipeline;

import com.datasqrl.config.EngineType;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.server.ServerEngine;
import com.datasqrl.util.StreamUtil;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface ExecutionPipeline {

  List<ExecutionStage> stages();

  default List<ExecutionStage> getReadStages() {
    return stages().stream().filter(ExecutionStage::isRead).collect(Collectors.toList());
  }

  /**
   * An execution pipeline can only have a single server engine
   *
   * @return
   */
  default Optional<ServerEngine> getServerEngine() {
    return StreamUtil.getOnlyElement(
        getStagesByType(EngineType.SERVER).stream()
            .map(stage -> (ServerEngine) stage.engine())
            .distinct());
  }

  default boolean hasReadStages() {
    return stages().stream().anyMatch(ExecutionStage::isRead);
  }

  Set<ExecutionStage> getUpStreamFrom(ExecutionStage stage);

  Set<ExecutionStage> getDownStreamFrom(ExecutionStage stage);

  default Optional<ExecutionStage> getStage(String name) {
    return StreamUtil.getOnlyElement(
        stages().stream().filter(s -> s.name().equalsIgnoreCase(name)));
  }

  default Optional<ExecutionStage> getMutationStage() {
    return StreamUtil.getOnlyElement(
        getStagesByType(EngineType.LOG).stream()
            .filter(stage -> stage.engine().supports(EngineFeature.MUTATIONS)));
  }

  default Optional<ExecutionStage> getStageByType(EngineType type) {
    return StreamUtil.getOnlyElement(
        stages().stream().filter(s -> s.engine().getType().equals(type)));
  }

  /**
   * We currently make the simplifying assumption that an {@link ExecutionPipeline} contains at most
   * one stage for any {@link EngineType}. This is not true in full generality and requires
   * significant changes to the DAGPlanner and import mechanism to support.
   *
   * @param type
   * @return the stage for a given {@link EngineType}.
   */
  default List<ExecutionStage> getStagesByType(EngineType type) {
    return stages().stream()
        .filter(s -> s.engine().getType().equals(type))
        .collect(Collectors.toList());
  }
}
