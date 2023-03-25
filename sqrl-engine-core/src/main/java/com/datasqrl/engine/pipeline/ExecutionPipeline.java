/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.pipeline;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.util.StreamUtil;
import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public interface ExecutionPipeline {

  Collection<ExecutionStage> getStages();

  default Set<ExecutionStage> getFrontendStages() {
    return getStages().stream().filter(s -> {
      switch (s.getEngine().getType()) {
        case DATABASE:
        case SERVER:
          return true;
        default:
          return false;
      }
    }).collect(Collectors.toSet());
  }

  Set<ExecutionStage> getUpStreamFrom(ExecutionStage stage);

  Set<ExecutionStage> getDownStreamFrom(ExecutionStage stage);

  default Optional<ExecutionStage> getStage(String name) {
    return StreamUtil.getOnlyElement(getStages().stream().filter(s -> s.getName().equalsIgnoreCase(name)));
  }

  /**
   * We currently make the simplifying assumption that an {@link ExecutionPipeline} contains at most
   * one stage for any {@link ExecutionEngine.Type}. This is not true in full generality and
   * requires significant changes to the DAGPlanner and import mechanism to support.
   *
   * @param type
   * @return the stage for a given {@link ExecutionEngine.Type}.
   */
  default Optional<ExecutionStage> getStage(ExecutionEngine.Type type) {
    return StreamUtil.getOnlyElement(getStages().stream().filter(s -> s.getEngine().getType().equals(type)));
  }


}
