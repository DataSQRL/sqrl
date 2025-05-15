package com.datasqrl.planner.dag.nodes;

import java.util.Map;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.tables.SqrlTableFunction;

import lombok.Getter;

/**
 * Represents a table function in the DAG
 */
@Getter
public class TableFunctionNode extends PlannedNode {

  private final SqrlTableFunction function;

  public TableFunctionNode(SqrlTableFunction function, Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    super("function", stageAnalysis);
    this.function = function;
  }

  @Override
  public String getId() {
    /*
    Access only functions are not planned and therefore do not have a unique object identifier,
    instead they are identified by their full path
     */
    return function.getVisibility().isAccessOnly()?
        "access:"+function.getFullPath():
        function.getIdentifier().asSummaryString();
//        + "(" + function.getParameters().stream().map(
//        FunctionParameter::getName).collect(Collectors.joining(",")) + ")";
  }

  @Override
  public boolean isSink() {
    return function.getVisibility().isEndpoint();
  }

  @Override
  public TableAnalysis getAnalysis() {
    return function.getFunctionAnalysis();
  }

}
