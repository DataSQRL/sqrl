/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.pipeline;

import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.util.StreamUtil;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface ExecutionPipeline {

  List<ExecutionStage> getStages();

  default List<ExecutionStage> getReadStages() {
    return getStages().stream().filter(ExecutionStage::isRead).collect(Collectors.toList());
  }

  default boolean hasReadStages() {
    return getStages().stream().anyMatch(ExecutionStage::isRead);
  }

  Set<ExecutionStage> getUpStreamFrom(ExecutionStage stage);

  Set<ExecutionStage> getDownStreamFrom(ExecutionStage stage);

  default Optional<ExecutionStage> getStage(String name) {
    return StreamUtil.getOnlyElement(getStages().stream().filter(s -> s.getName().equalsIgnoreCase(name)));
  }
  default Optional<ExecutionStage> getStageByType(String type) {
    return StreamUtil.getOnlyElement(getStages().stream().filter(s -> s.getEngine().getType().name().equalsIgnoreCase(type)));
  }

  /**
   * We currently make the simplifying assumption that an {@link ExecutionPipeline} contains at most
   * one stage for any {@link Type}. This is not true in full generality and
   * requires significant changes to the DAGPlanner and import mechanism to support.
   *
   * @param type
   * @return the stage for a given {@link Type}.
   */
  default Optional<List<ExecutionStage>> getStage(Type type) {
    List<ExecutionStage> executionStageStream = getStages().stream()
        .filter(s -> s.getEngine().getType().equals(type))
        .collect(Collectors.toList());
    return executionStageStream.isEmpty() ? Optional.empty() : Optional.of(executionStageStream);
  }


}
