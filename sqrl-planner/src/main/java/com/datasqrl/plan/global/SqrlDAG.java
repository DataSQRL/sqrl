package com.datasqrl.plan.global;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.calcite.table.ScriptRelationalTable;
import com.datasqrl.plan.global.SqrlDAG.SqrlNode;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.util.AbstractDAG;
import com.datasqrl.util.StreamUtil;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.Value;

public class SqrlDAG extends AbstractDAG<SqrlNode, SqrlDAG> {


  protected SqrlDAG(Multimap<SqrlNode, SqrlNode> inputs) {
    super(inputs);
  }

  @Override
  protected SqrlDAG create(Multimap<SqrlNode, SqrlNode> inputs) {
    return new SqrlDAG(inputs);
  }

  public void eliminateInviableStages(ExecutionPipeline pipeline) {
    messagePassing(node -> {
      boolean hasChange = node.stageAnalysis.values().stream().filter(s -> s.isSupported())
          .map( stageAnalysis -> {
            ExecutionStage stage = stageAnalysis.getStage();
            //Each input/output node must have a viable upstream/downstream stage, otherwise this stage isn't viable
            for (boolean upstream : new boolean[]{true, false}) {
              Set<ExecutionStage> compatibleStages = upstream?pipeline.getUpStreamFrom(stage):
                  pipeline.getDownStreamFrom(stage);
              Optional<SqrlNode> noCompatibleStage = (upstream?getInputs(node):getOutputs(node)).
                  stream().filter(ngh -> !ngh.stageAnalysis.values().stream().anyMatch(
                      sa -> sa.isSupported() && compatibleStages.contains(sa.getStage()))
                  ).findAny();
              if (noCompatibleStage.isPresent()) {
                node.stageAnalysis.put(stage, new StageAnalysis.MissingDependent(stage, upstream, noCompatibleStage.get().getName()));
                return true;
              }
            }
            return false;
          }).anyMatch(Boolean::booleanValue);
      if (!node.hasViableStage()) throw new NoPlanException(node);
      return false;
    },100);
  }

  @Value
  public static class NoPlanException extends RuntimeException {

    SqrlNode node;

  }

  @AllArgsConstructor
  public abstract static class SqrlNode implements Node {

    private final Map<ExecutionStage, StageAnalysis> stageAnalysis;

    public abstract String getName();

    public boolean hasViableStage() {
      return stageAnalysis.values().stream().anyMatch(stage -> stage.isSupported());
    }

    /**
     * Sets the execution stage with the lowest cost and eliminates all others.
     *
     * @return true, if other stages were eliminated, else false
     */
    public boolean setCheapestStage() {
      Optional<StageAnalysis.Cost> stage = StreamUtil.filterByClass(stageAnalysis.values(),
              StageAnalysis.Cost.class)
          .sorted((s1, s2) -> s1.getCost().compareTo(s2.getCost())).findFirst();
      Preconditions.checkArgument(stage.isPresent());
      StageAnalysis.Cost cheapest = stage.get();
      return StreamUtil.filterByClass(stageAnalysis.values(),
          StageAnalysis.Cost.class).filter(other -> !cheapest.equals(other))
          .map(other ->
              stageAnalysis.put(other.getStage(), other.tooExpensive())).count()>0;
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

  }

  @Value
  public static class TableNode extends SqrlNode {

    private final ScriptRelationalTable table;

    public TableNode(Map<ExecutionStage, StageAnalysis> stageAnalysis,
        ScriptRelationalTable table) {
      super(stageAnalysis);
      this.table = table;
    }

    @Override
    public String getName() {
      return "table " + table.getNameId();
    }

  }

  @Value
  public static class QueryNode extends SqrlNode {

    private final AnalyzedAPIQuery query;

    public QueryNode(Map<ExecutionStage, StageAnalysis> stageAnalysis, AnalyzedAPIQuery query) {
      super(stageAnalysis);
      this.query = query;
    }

    @Override
    public String getName() {
      return "query " + query.getBaseQuery().getNameId();
    }

    @Override
    public boolean isSink() {
      return true;
    }
  }

  @Value
  public static class ExportNode extends SqrlNode {

    private final ResolvedExport export;
    private final String uniqueId;

    public ExportNode(Map<ExecutionStage, StageAnalysis> stageAnalysis, ResolvedExport export,
        String uniqueId) {
      super(stageAnalysis);
      this.export = export;
      this.uniqueId = uniqueId;
    }

    @Override
    public String getName() {
      return "export " + uniqueId;
    }

    @Override
    public boolean isSink() {
      return true;
    }

  }



}
