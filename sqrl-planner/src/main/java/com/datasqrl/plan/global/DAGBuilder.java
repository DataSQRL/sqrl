package com.datasqrl.plan.global;

import static com.datasqrl.plan.global.DAGAssembler.getExportBaseConfig;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.calcite.rel.RelNode;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.engine.EngineFeature;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.SqrlDAG.ExportNode;
import com.datasqrl.plan.global.SqrlDAG.QueryNode;
import com.datasqrl.plan.global.SqrlDAG.SqrlNode;
import com.datasqrl.plan.global.SqrlDAG.TableNode;
import com.datasqrl.plan.global.StageAnalysis.Cost;
import com.datasqrl.plan.rules.ComputeCost;
import com.datasqrl.plan.rules.ExecutionAnalysis;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SQRLConverter.TablePlan;
import com.datasqrl.plan.rules.SimpleCostModel;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.inject.Inject;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * Assembles the DAG from the sinks and tables
 */
@AllArgsConstructor(onConstructor_=@Inject)
@Getter
public class DAGBuilder {

  private SQRLConverter sqrlConverter;
  private ExecutionPipeline pipeline;
  private ErrorCollector errors;

  public SqrlDAG build(Collection<AnalyzedAPIQuery> queries,
      Collection<AnalyzedExport> exports) {
    Map<PhysicalRelationalTable, TableNode> table2Node = new HashMap<>();
    Multimap<SqrlNode, SqrlNode> dagInputs = HashMultimap.create();
    //1. Add all queries as sinks
    var readStages = pipeline.getReadStages();
    if (!readStages.isEmpty()) {
      for (AnalyzedAPIQuery query : queries) {
        add2DAG(query.getRelNode(), query.getBaseConfig(), readStages, dagInputs,
            stageAnalysis -> new QueryNode(stageAnalysis, query), table2Node);
      }
    }
    //2. Add all exports as sinks
    var numExports = 1;
    List<ExecutionStage> exportStages = pipeline.getStages().stream().filter(s -> s.supportsFeature(
        EngineFeature.EXPORT)).collect(Collectors.toList());
    errors.checkFatal(!exportStages.isEmpty(), "Configured Pipeline does not include "
        + "any stages that support export: %s",pipeline);
    for (AnalyzedExport export : exports) {
      var name = Name.addSuffix(export.getTable(), String.valueOf(numExports++));
      add2DAG(export.getRelNode(), getExportBaseConfig(), exportStages, dagInputs,
          stageAnalysis -> new ExportNode(stageAnalysis, export, name), table2Node);
    }
    return new SqrlDAG(dagInputs);
  }

  private void add2DAG(RelNode relnode, SqrlConverterConfig baseConfig,
      List<ExecutionStage> stages, Multimap<SqrlNode, SqrlNode> dagInputs,
      Function<Map<ExecutionStage, StageAnalysis>,SqrlNode> nodeConstructor,
      Map<PhysicalRelationalTable, TableNode> table2Node) {
    Set<PhysicalRelationalTable> inputTables = new LinkedHashSet<>();
    var configBuilder = baseConfig.toBuilder()
        .sourceTableConsumer(inputTables::add);

    //Try all stages to determine which one are viable
    var stageAnalysis = tryStages(stages, stage ->
        TablePlan.of(sqrlConverter.convert(relnode, configBuilder.stage(stage).build(), errors.onlyErrors())));
    var node = nodeConstructor.apply(stageAnalysis);
    //Add all input nodes
    inputTables.stream().map(table -> getInputTable(table, dagInputs, table2Node)).forEach( input ->
        dagInputs.put(node,input));
  }

  private SqrlDAG.TableNode getInputTable(PhysicalRelationalTable table,
                                          Multimap<SqrlNode, SqrlNode> dagInputs,
                                          Map<PhysicalRelationalTable, TableNode> table2Node) {
    if (table2Node.containsKey(table)) {
        return table2Node.get(table);
    }
    Set<PhysicalRelationalTable> inputTables = new LinkedHashSet<>();
    var configBuilder = table.getBaseConfig();
    configBuilder.sourceTableConsumer(inputTables::add);
    var stages = table.getSupportedStages(pipeline, errors);
    var stageAnalysis = tryStages(stages, stage ->
        sqrlConverter.convert(table, configBuilder.stage(stage).build(), errors.onlyErrors()));
    var node = new TableNode(stageAnalysis, table);
    table2Node.put(table,node);
    //Since it's a DAG, we can recursively add tables to source without running the risk of a loop
    inputTables.stream().map(inputTbl -> getInputTable(inputTbl, dagInputs, table2Node)).forEach( input ->
        dagInputs.put(node,input));
    return node;
  }

  private Map<ExecutionStage, StageAnalysis> tryStages(List<ExecutionStage> stages,
      Function<ExecutionStage, SQRLConverter.TablePlan> planner) {
    Map<ExecutionStage, StageAnalysis> stageAnalysis = new LinkedHashMap<>();
    for (ExecutionStage stage : stages) {
      if (!stage.isCompute()) {
        continue;
    }
      StageAnalysis result;
      try {
        var plan = planner.apply(stage);
        ComputeCost cost = SimpleCostModel.of(stage, plan.getRelNode());
        result = new Cost(stage, cost, true);
      } catch (ExecutionAnalysis.CapabilityException ex) {
        result = StageAnalysis.of(ex);
      }
      stageAnalysis.put(stage, result);
    }
    return stageAnalysis;
  }


}
