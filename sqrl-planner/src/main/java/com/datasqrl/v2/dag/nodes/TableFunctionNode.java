package com.datasqrl.v2.dag.nodes;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.analyzer.TableOrFunctionAnalysis.FullIdentifier;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.datasqrl.plan.global.StageAnalysis;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.schema.FunctionParameter;

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
