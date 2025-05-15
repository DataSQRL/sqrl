package com.datasqrl.planner.dag;

import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.plan.global.StageAnalysis.MissingDependent;
import com.datasqrl.util.AbstractDAG;
import com.datasqrl.planner.dag.nodes.PipelineNode;
import com.google.common.collect.Multimap;

import lombok.Value;

/**
 * A DAG that consists of {@link PipelineNode}s.
 */
public class PipelineDAG extends AbstractDAG<PipelineNode, PipelineDAG> {


  protected PipelineDAG(Multimap<PipelineNode, PipelineNode> inputs) {
    super(inputs);
  }

  @Override
  protected PipelineDAG create(Multimap<PipelineNode, PipelineNode> inputs) {
    return new PipelineDAG(inputs);
  }

  @Override
  public<T extends PipelineNode> Stream<T> allNodesByClass(Class<T> clazz) {
    return super.allNodesByClass(clazz).sorted();
  }

  public<T extends PipelineNode> Stream<T> allNodesByClassAndStage(Class<T> clazz, ExecutionStage stage) {
    return super.allNodesByClass(clazz).filter(node -> node.getChosenStage().equals(stage)).sorted();
  }

  /**
   * Iterates through the DAG from source to sink and eliminates inviable stages.
   * A stage s is not viable if an upstream node in the DAG does not support an upstream stage of s.
   * A stage s is not viable if a downstream node in the DAG does not support a downstream stage of s.
   *
   * In less formal terms, a stage is not viable if the inputs and outputs cannot be inputs or outputs
   * for that particular stage.
   *
   * @param pipeline
   */
  public void eliminateInviableStages(ExecutionPipeline pipeline) {
    messagePassing(node -> {
      final var updatedStages = new LinkedHashMap<ExecutionStage, StageAnalysis>();
      var hasChange = node.getStageAnalysis().values().stream().filter(s -> s.isSupported())
          .map( stageAnalysis -> {
            ExecutionStage stage = stageAnalysis.getStage();
            //Each input/output node must have a viable upstream/downstream stage, otherwise this stage isn't viable
            for (boolean upstream : new boolean[]{true, false}) {
              Set<ExecutionStage> compatibleStages = upstream?pipeline.getUpStreamFrom(stage):
                  pipeline.getDownStreamFrom(stage);
              Optional<PipelineNode> noCompatibleStage = (upstream?getInputs(node):getOutputs(node)).
                  stream().filter(ngh -> !ngh.getStageAnalysis().values().stream().anyMatch(
                      sa -> sa.isSupported() && compatibleStages.contains(sa.getStage()))
                  ).findAny();
              if (noCompatibleStage.isPresent()) {
                updatedStages.put(stage, new MissingDependent(stage, upstream, noCompatibleStage.get().getName()));
                return true;
              }
            }
            return false;
          }).anyMatch(Boolean::booleanValue);
      updatedStages.entrySet().stream()
          .forEach(e-> node.getStageAnalysis().put(e.getKey(), e.getValue()));

      if (!node.hasViableStage()) {
        throw new NoPlanException(node);
      }
      return hasChange;
    },100);
  }

  @Value
  public static class NoPlanException extends RuntimeException {

    PipelineNode node;

  }

}
