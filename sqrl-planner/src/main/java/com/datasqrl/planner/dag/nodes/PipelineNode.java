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

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.util.AbstractDAG;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.Comparator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;

/**
 * Abstract Node in the {@link com.datasqrl.planner.dag.PipelineDAG} which represents a table or
 * function in the computational graph of a SQRL script.
 */
@AllArgsConstructor
public abstract class PipelineNode implements AbstractDAG.Node, Comparable<PipelineNode> {

  private final @NonNull String type;

  @Getter private final Map<ExecutionStage, StageAnalysis> stageAnalysis;

  @Override
  public String getName() {
    return type + " " + getId();
  }

  public abstract String getId();

  public boolean hasViableStage() {
    return stageAnalysis.values().stream().anyMatch(stage -> stage.isSupported());
  }

  public <C extends PipelineNode> C unwrap(Class<C> clazz) {
    Preconditions.checkArgument(clazz.isInstance(this));
    return (C) this;
  }

  /**
   * Sets the execution stage with the lowest cost and eliminates all others.
   *
   * @return true, if other stages were eliminated, else false
   */
  public boolean setCheapestStage() {
    var cheapest = findCheapestStage(stageAnalysis);
    return StreamUtil.filterByClass(stageAnalysis.values(), StageAnalysis.Cost.class)
            .filter(other -> !cheapest.equals(other))
            .map(other -> stageAnalysis.put(other.getStage(), other.tooExpensive()))
            .count()
        > 0;
  }

  public static StageAnalysis.Cost findCheapestStage(
      Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    var stage =
        StreamUtil.filterByClass(stageAnalysis.values(), StageAnalysis.Cost.class)
            .sorted(Comparator.comparing(StageAnalysis.Cost::getCost))
            .findFirst();
    Preconditions.checkArgument(stage.isPresent());
    return stage.get();
  }

  public ExecutionStage getChosenStage() {
    return Iterables.getOnlyElement(
            Iterables.filter(stageAnalysis.values(), StageAnalysis::isSupported))
        .getStage();
  }

  @Override
  public String toString() {
    return getName() + " - " + stageAnalysisToString();
  }

  public String stageAnalysisToString() {
    if (stageAnalysis.isEmpty()) {
      return "no stages found";
    }
    return stageAnalysis.values().stream()
        .map(stage -> stage.getStage().name() + ": " + stage.getMessage())
        .collect(Collectors.joining("\n"));
  }

  @Override
  public int compareTo(PipelineNode other) {
    return getName().compareTo(other.getName());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    var that = (PipelineNode) o;
    return Objects.equals(getName(), that.getName());
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(getName());
  }
}
