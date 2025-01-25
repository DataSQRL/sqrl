package com.datasqrl.flinkwrapper.dag;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.flinkwrapper.dag.nodes.PipelineNode;
import com.datasqrl.flinkwrapper.dag.nodes.TableNode;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.SqrlDAG;
import com.datasqrl.plan.table.PhysicalTable;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class DAGPlanner {

  private final ExecutionPipeline pipeline;
  private final ErrorCollector errors;

  public PipelineDAG optimize(PipelineDAG dag) {
    dag = dag.trimToSinks();
    for (PipelineNode node : dag) {
      if (!node.hasViableStage()) {
        errors.fatal("Could not find execution stage for [%s]. Stage analysis below.\n%s",node.getName(), node.toString());
      }
    }
    try {
      dag.eliminateInviableStages(pipeline);
    } catch (PipelineDAG.NoPlanException ex) {
      //Print error message that is easy to read
      errors.fatal("Could not find execution stage for [%s]. Full DAG below.\n%s", ex.getNode().getName(), dag);
    }
    dag.forEach(node -> Preconditions.checkArgument(node.hasViableStage()));

    //Pick most cost-effective stage for each node and assign
    for (PipelineNode node : dag) {
      if (node.setCheapestStage()) {
        //If we eliminated stages, we make sure to eliminate all inviable stages
        dag.eliminateInviableStages(pipeline);
      }
      //Assign stage to table
      if (node instanceof TableNode) {
        TableAnalysis table = ((TableNode) node).getTableAnalysis();
        ExecutionStage stage = node.getChosenStage();
        Preconditions.checkNotNull(stage);
        //table.assignStage(stage); //this stage on the config below
      }
    }

    return dag;
  }

  public PhysicalDAGPlan assemble(PipelineDAG dag) {
    //move assembler logic here
    //1st: find all the cuts between flink and materialization (db+log) stages or sinks
    //generate a sink for each in the respective engine and insert into

    //2nd: for each materialization stage, find all cuts to server or sinks
    //generate queries for those

    //3rd: build table functions for server stage: for now, those are trivial

    return null;
  }

}
