package com.datasqrl.v2.dag.nodes;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.datasqrl.plan.global.StageAnalysis;
import java.util.Map;
import lombok.Getter;

@Getter
public class TableFunctionNode extends PlannedNode {

  private final SqrlTableFunction function;

  public TableFunctionNode(SqrlTableFunction function, Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    super("function", stageAnalysis);
    this.function = function;
  }

  @Override
  public String getId() {
    return function.getFullPath().toString();
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
