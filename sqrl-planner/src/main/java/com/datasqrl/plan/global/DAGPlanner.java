/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.plan.global;

import com.datasqrl.calcite.OperatorTable;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.plan.local.generate.ResolvedExport;
import com.datasqrl.plan.rules.SqrlConverterConfig;
import com.datasqrl.util.FunctionUtil;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import com.datasqrl.plan.rules.SQRLConverter;
import com.datasqrl.plan.table.PhysicalTable;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlOperator;
import org.apache.flink.table.functions.FunctionDefinition;

/**
 * The DAGPlanner currently makes the simplifying assumption that the execution pipeline consists of
 * two stages: stream and database.
 */
@AllArgsConstructor(onConstructor_=@Inject)
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
    //Prepare the inputs
    Collection<AnalyzedAPIQuery> analyzedQueries = dagPreparation.prepareInputs(framework.getSchema(), exports);

    //Assemble DAG
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

  public void optimize(SqrlDAG dag) {
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
      SqrlConverterConfig config = table.getBaseConfig().build();
      table.setPlannedRelNode(sqrlConverter.convert(table, config, errors));
    });
  }

  public PhysicalDAGPlan assemble(SqrlDAG dag,
      Set<URL> jars, Map<String, UserDefinedFunction> udfs) {
    //Stitch DAG together
    return assembler.assemble(dag, jars, udfs);
  }

  public SqrlDAG planLogical() {
    List<ResolvedExport> exports = framework.getSchema().getExports();
    SqrlDAG dag = build(exports);
    optimize(dag);
    return dag;
  }

  public PhysicalDAGPlan planPhysical(SqrlDAG dag) {
    return assemble(dag, framework.getSchema().getJars(),
        extractFlinkFunctions(framework.getSqrlOperatorTable()));
  }

  public PhysicalDAGPlan plan() {
    return assemble(planLogical(), framework.getSchema().getJars(),
        extractFlinkFunctions(framework.getSqrlOperatorTable()));
  }

  public Map<String, UserDefinedFunction> extractFlinkFunctions(
      OperatorTable sqrlOperatorTable) {
    Map<String, UserDefinedFunction> fncs = new HashMap<>();
    for (Map.Entry<String, SqlOperator> fnc : sqrlOperatorTable.getUdfs().entrySet()) {
      Optional<FunctionDefinition> definition = FunctionUtil.getBridgedFunction(fnc.getValue());
      if (definition.isPresent()) {
        if (definition.get() instanceof UserDefinedFunction) {
          fncs.put(fnc.getKey(), (UserDefinedFunction)definition.get());
        }
      }
    }
    return fncs;
  }
}
