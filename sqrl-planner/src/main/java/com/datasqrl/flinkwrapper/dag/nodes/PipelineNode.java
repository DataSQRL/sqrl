package com.datasqrl.flinkwrapper.dag.nodes;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.plan.global.StageAnalysis.Cost;
import com.datasqrl.util.AbstractDAG;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.NonNull;

public abstract class PipelineNode implements AbstractDAG.Node, Comparable<PipelineNode> {

  private final String type;

  @Getter
  Map<ExecutionStage, StageAnalysis> stageAnalysis = null;

  protected PipelineNode(@NonNull String type) {
    this.type = type;
  }

  public String getName() {
    return type + " " + getId();
  }

  public abstract String getId();

  public boolean hasViableStage() {
    return stageAnalysis.values().stream().anyMatch(stage -> stage.isSupported());
  }

  /**
   * Sets the execution stage with the lowest cost and eliminates all others.
   *
   * @return true, if other stages were eliminated, else false
   */
  public boolean setCheapestStage() {
    StageAnalysis.Cost cheapest = findCheapestStage(stageAnalysis);
    return StreamUtil.filterByClass(stageAnalysis.values(),
            StageAnalysis.Cost.class).filter(other -> !cheapest.equals(other))
        .map(other ->
            stageAnalysis.put(other.getStage(), other.tooExpensive())).count()>0;
  }

  public static StageAnalysis.Cost findCheapestStage(Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    Optional<Cost> stage = StreamUtil.filterByClass(stageAnalysis.values(),
            StageAnalysis.Cost.class)
        .sorted(Comparator.comparing(StageAnalysis.Cost::getCost)).findFirst();
    Preconditions.checkArgument(stage.isPresent());
    return stage.get();
  }

  public ExecutionStage getChosenStage() {
    return Iterables.getOnlyElement(Iterables.filter(stageAnalysis.values(),
        StageAnalysis::isSupported)).getStage();
  }

  @Override
  public String toString() {
    if (stageAnalysis.isEmpty()) {
      return "no stages found";
    }
    return stageAnalysis.values().stream().map( stage ->
        stage.getStage().getName() + ": " + stage.getMessage()
    ).collect(Collectors.joining("\n"));
  }

  @Override
  public int compareTo(PipelineNode other) {
    return getName().compareTo(other.getName());
  }


}
