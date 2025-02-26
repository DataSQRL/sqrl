/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.datasqrl.plan.table.PhysicalTable;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;

/**
 * The DAGPlanner currently makes the simplifying assumption that the execution pipeline consists of
 * two stages: stream and database.
 */
@AllArgsConstructor(onConstructor_ = @Inject)
public class DAGPlanner {

  private final SqrlFramework framework;
  private final APIConnectorManager apiManager;
  private final ExecutionPipeline pipeline;
  private final ErrorCollector errors;
  private final DAGAssembler assembler;
  private final DAGBuilder dagBuilder;
  private final DAGPreparation dagPreparation;
  private final SQRLConverter sqrlConverter;

  public SqrlDAG build(Collection<ResolvedExport> exports) {
    // Prepare the inputs
    DAGPreparation.Result prepResult = dagPreparation.prepareInputs(framework.getSchema(), exports);

    // Assemble DAG
    SqrlDAG dag =
        new DAGBuilder(sqrlConverter, pipeline, errors)
            .build(prepResult.getQueries(), prepResult.getExports());
    for (SqrlDAG.SqrlNode node : dag) {
      if (!node.hasViableStage()) {
        errors.fatal(
            "Could not find execution stage for [%s]. Stage analysis below.\n%s",
            node.getName(), node.toString());
      }
    }
    try {
      dag.eliminateInviableStages(pipeline);
    } catch (SqrlDAG.NoPlanException ex) {
      // Print error message that is easy to read
      errors.fatal(
          "Could not find execution stage for [%s]. Full DAG below.\n%s",
          ex.getNode().getName(), dag);
    }
    dag.forEach(node -> Preconditions.checkArgument(node.hasViableStage()));
    return dag;
  }

  public void optimize(SqrlDAG dag) {
    // Pick most cost-effective stage for each node and assign
    for (SqrlDAG.SqrlNode node : dag) {
      if (node.setCheapestStage()) {
        // If we eliminated stages, we make sure to eliminate all inviable stages
        dag.eliminateInviableStages(pipeline);
      }
      // Assign stage to table
      if (node instanceof SqrlDAG.TableNode) {
        PhysicalTable table = ((SqrlDAG.TableNode) node).getTable();
        ExecutionStage stage = node.getChosenStage();
        Preconditions.checkNotNull(stage);
        table.assignStage(stage); // this stage on the config below
      }
    }
    // Plan final version of all tables
    dag.allNodesByClass(SqrlDAG.TableNode.class)
        .forEach(
            tableNode -> {
              PhysicalTable table = tableNode.getTable();
              SqrlConverterConfig config = table.getBaseConfig().build();
              table.setPlannedRelNode(sqrlConverter.convert(table, config, errors.onlyErrors()));
            });
  }

  public PhysicalDAGPlan assemble(SqrlDAG logicalDag, Set<URL> jars) {
    // Stitch DAG together
    return assembler.assemble(logicalDag, jars);
  }

  public SqrlDAG planLogical() {
    List<ResolvedExport> exports = framework.getSchema().getExports();
    SqrlDAG dag = build(exports);
    optimize(dag);
    return dag;
  }

  public PhysicalDAGPlan planPhysical(SqrlDAG dag) {
    return assemble(dag, framework.getSchema().getJars());
  }

  public PhysicalDAGPlan plan() {
    return assemble(planLogical(), framework.getSchema().getJars());
  }
}
