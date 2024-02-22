/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorLookup;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.PhysicalTable;
import com.google.common.base.Preconditions;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.net.URL;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * The DAGPlanner currently makes the simplifying assumption that the execution pipeline consists of
 * two stages: stream and database.
 */
public class DAGPlanner {

  public static SqrlDAG build(SqrlFramework framework, APIConnectorLookup apiManager,
      Collection<ResolvedExport> exports, ExecutionPipeline pipeline, ErrorCollector errors) {
    //Prepare the inputs
    Collection<AnalyzedAPIQuery> analyzedQueries = new DAGPreparation(framework.getQueryPlanner().getRelBuilder(),
        errors).prepareInputs(framework.getSchema(), apiManager, exports);

    //Assemble DAG
    SQRLConverter sqrlConverter = new SQRLConverter(framework.getQueryPlanner().getRelBuilder());
    SqrlDAG dag = new DAGBuilder(sqrlConverter, pipeline, errors).build(analyzedQueries, exports);
    for (SqrlDAG.SqrlNode node : dag) {
      if (!node.hasViableStage()) {
        errors.fatal("Could not find execution stage for [%s]. Stage analysis below.\n%s",node.getName(), node.toString());
      }
    }
    try {
      dag.eliminateInviableStages(pipeline);
    } catch (SqrlDAG.NoPlanException ex) {
      //Print error message that is easy to read
      errors.fatal("Could not find execution stage for [%s]. Full DAG below.\n%s", ex.getNode().getName(), dag);
    }
    dag.forEach(node -> Preconditions.checkArgument(node.hasViableStage()));
    return dag;
  }

  public static void optimize(SqrlDAG dag, ExecutionPipeline pipeline, SQRLConverter sqrlConverter, ErrorCollector errors) {
    //Pick most cost-effective stage for each node and assign
    for (SqrlDAG.SqrlNode node : dag) {
      if (node.setCheapestStage()) {
        //If we eliminated stages, we make sure to eliminate all inviable stages
        dag.eliminateInviableStages(pipeline);
      }
      //Assign stage to table
      if (node instanceof SqrlDAG.TableNode) {
        PhysicalTable table = ((SqrlDAG.TableNode) node).getTable();
        ExecutionStage stage = node.getChosenStage();
        Preconditions.checkNotNull(stage);
        table.assignStage(stage); //this stage on the config below
      }
    }
    //Plan final version of all tables
    dag.allNodesByClass(SqrlDAG.TableNode.class).forEach( tableNode -> {
      PhysicalTable table = tableNode.getTable();
      SQRLConverter.Config config = table.getBaseConfig().build();
      table.setPlannedRelNode(sqrlConverter.convert(table, config, errors));
    });
  }

  public static PhysicalDAGPlan assemble(SqrlDAG dag, APIConnectorLookup apiManager,
      Set<URL> jars, Map<String, UserDefinedFunction> udfs, RootGraphqlModel model,
      SqrlFramework framework, SQRLConverter sqrlConverter, ExecutionPipeline pipeline,
      Debugger debugger, ErrorCollector errors) {
    //Stitch DAG together
    DAGAssembler assembler = new DAGAssembler(framework, framework.getQueryPlanner().getPlanner(),
        sqrlConverter, pipeline, debugger, errors);
    return assembler.assemble(dag, jars, udfs, model, apiManager);
  }

  public static SqrlDAG planLogical(SqrlFramework framework, APIConnectorLookup apiManager,
                                    Collection<ResolvedExport> exports, ExecutionPipeline pipeline, ErrorCollector errors) {
    SqrlDAG dag = build(framework, apiManager, exports, pipeline, errors);
    SQRLConverter converter = new SQRLConverter(framework.getQueryPlanner().getRelBuilder());
    optimize(dag, pipeline, converter, errors);
    return dag;
  }

  public static PhysicalDAGPlan planPhysical(SqrlFramework framework, APIConnectorLookup apiManager,
                                             Collection<ResolvedExport> exports, Set<URL> jars, Map<String, UserDefinedFunction> udfs,
                                             RootGraphqlModel model, ExecutionPipeline pipeline, ErrorCollector errors,
                                             Debugger debugger) {
    SqrlDAG dag = planLogical(framework, apiManager, exports, pipeline, errors);
    return assemble(dag, apiManager, jars, udfs, model, framework, new SQRLConverter(framework.getQueryPlanner().getRelBuilder()),
        pipeline, debugger, errors);
  }
}
