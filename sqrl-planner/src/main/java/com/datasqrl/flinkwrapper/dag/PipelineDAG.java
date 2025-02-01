package com.datasqrl.flinkwrapper.dag;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.flinkwrapper.dag.nodes.PipelineNode;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.plan.global.StageAnalysis.MissingDependent;
import com.datasqrl.util.AbstractDAG;
import com.google.common.collect.Multimap;
import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import lombok.Value;

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

  public void eliminateInviableStages(ExecutionPipeline pipeline) {
    messagePassing(node -> {
      final LinkedHashMap<ExecutionStage, StageAnalysis> updatedStages = new LinkedHashMap<>();
      boolean hasChange = node.getStageAnalysis().values().stream().filter(s -> s.isSupported())
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
