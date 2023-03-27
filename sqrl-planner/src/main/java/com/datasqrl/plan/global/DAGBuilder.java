package com.datasqrl.plan.global;

import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.name.Name;
import com.datasqrl.plan.calcite.rules.ComputeCost;
import com.datasqrl.plan.calcite.rules.ExecutionAnalysis;
import com.datasqrl.plan.calcite.rules.SQRLConverter;
import com.datasqrl.plan.calcite.rules.SQRLConverter.Config;
import com.datasqrl.plan.calcite.rules.SimpleCostModel;
import com.datasqrl.plan.calcite.table.ScriptRelationalTable;
import com.datasqrl.plan.global.SqrlDAG.ExportNode;
import com.datasqrl.plan.global.SqrlDAG.QueryNode;
import com.datasqrl.plan.global.SqrlDAG.SqrlNode;
import com.datasqrl.plan.global.SqrlDAG.TableNode;
import com.datasqrl.plan.global.StageAnalysis.Cost;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import lombok.Value;
import org.apache.calcite.rel.RelNode;

/**
 * Assembles the DAG from the sinks and tables
 */
@Value
public class DAGBuilder {

  private SQRLConverter sqrlConverter;
  private ExecutionPipeline pipeline;
  private ErrorCollector errors;


  public SqrlDAG build(Collection<AnalyzedAPIQuery> queries,
      Collection<ResolvedExport> exports) {
    Map<ScriptRelationalTable, TableNode> table2Node = new HashMap<>();
    Multimap<SqrlNode, SqrlNode> dagInputs = HashMultimap.create();
    //1. Add all queries as sinks
    Set<ExecutionStage> frontendStages = pipeline.getFrontendStages();
    errors.checkFatal(!frontendStages.isEmpty(), "Configured Pipeline does not include"
        + " any frontend stages to execute API queries: %s",pipeline);
    for (AnalyzedAPIQuery query : queries) {
      add2DAG(query.getRelNode(), query.getBaseConfig(), frontendStages, dagInputs,
          stageAnalysis -> new QueryNode(stageAnalysis, query), table2Node);
    }
    //2. Add all exports as sinks
    int numExports = 1;
    Set<ExecutionStage> exportStages = pipeline.getStages().stream().filter(s -> s.supports(
        EngineCapability.EXPORT)).collect(Collectors.toSet());
    errors.checkFatal(!exportStages.isEmpty(), "Configured Pipeline does not include "
        + "any stages that support export: %s",pipeline);
    for (ResolvedExport export : exports) {
      String name = Name.addSuffix(export.getTable().getNameId(), String.valueOf(numExports++));
      add2DAG(export.getRelNode(), export.getBaseConfig(), exportStages, dagInputs,
          stageAnalysis -> new ExportNode(stageAnalysis, export, name), table2Node);
    }
    return new SqrlDAG(dagInputs);
  }

  private void add2DAG(RelNode relnode, Config baseConfig,
      Collection<ExecutionStage> stages, Multimap<SqrlNode, SqrlNode> dagInputs,
      Function<Map<ExecutionStage, StageAnalysis>,SqrlNode> nodeConstructor,
      Map<ScriptRelationalTable, TableNode> table2Node) {
    Set<ScriptRelationalTable> inputTables = new LinkedHashSet<>();
    Config.ConfigBuilder configBuilder = baseConfig.toBuilder()
        .sourceTableConsumer(inputTables::add);

    //Try all stages to determine which one are viable
    Map<ExecutionStage, StageAnalysis> stageAnalysis = tryStages(stages, stage ->
        sqrlConverter.convert(relnode, configBuilder.stage(stage).build(), errors).getRelNode());
    SqrlNode node = nodeConstructor.apply(stageAnalysis);
    //Add all input nodes
    inputTables.stream().map(table -> getInputTable(table, dagInputs, table2Node)).forEach( input ->
        dagInputs.put(node,input));
  }

  private SqrlDAG.TableNode getInputTable(ScriptRelationalTable table,
      Multimap<SqrlNode, SqrlNode> dagInputs,
      Map<ScriptRelationalTable, TableNode> table2Node) {
    if (table2Node.containsKey(table)) return table2Node.get(table);
    Set<ScriptRelationalTable> inputTables = new LinkedHashSet<>();
    SQRLConverter.Config.ConfigBuilder configBuilder = table.getBaseConfig();
    configBuilder.sourceTableConsumer(inputTables::add);
    Collection<ExecutionStage> stages = table.getSupportedStages(pipeline, errors);
    Map<ExecutionStage, StageAnalysis> stageAnalysis = tryStages(stages, stage ->
        sqrlConverter.convert(table, configBuilder.stage(stage).build(), errors));
    TableNode node = new TableNode(stageAnalysis, table);
    table2Node.put(table,node);
    //Since it's a DAG, we can recursively add tables to source without running the risk of a loop
    inputTables.stream().map(inputTbl -> getInputTable(inputTbl, dagInputs, table2Node)).forEach( input ->
        dagInputs.put(node,input));
    return node;
  }

  public Map<ExecutionStage, StageAnalysis> planStages(ScriptRelationalTable table) {
    SQRLConverter.Config.ConfigBuilder configBuilder = table.getBaseConfig();
    Collection<ExecutionStage> stages = table.getSupportedStages(pipeline, errors);
    return tryStages(stages, stage ->
        sqrlConverter.convert(table, configBuilder.stage(stage).build(), errors));
  }

  private Map<ExecutionStage, StageAnalysis> tryStages(Collection<ExecutionStage> stages,
      Function<ExecutionStage, RelNode> planner) {
    Map<ExecutionStage, StageAnalysis> stageAnalysis = new HashMap<>();
    for (ExecutionStage stage : stages) {
      StageAnalysis result;
      try {
        RelNode relNode = planner.apply(stage);
        ComputeCost cost = SimpleCostModel.of(stage.getEngine().getType(), relNode);
        result = new Cost(stage, cost, true, relNode);
      } catch (ExecutionAnalysis.CapabilityException ex) {
        result = StageAnalysis.of(ex);
      }
      stageAnalysis.put(stage, result);
    }
    return stageAnalysis;
  }


}
