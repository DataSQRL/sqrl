package com.datasqrl.flinkwrapper.dag.nodes;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.flinkwrapper.tables.SqrlTableFunction;
import com.datasqrl.plan.global.StageAnalysis;
import java.util.Map;
import lombok.Getter;
import org.apache.flink.table.catalog.ObjectIdentifier;

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
    return function.getVisibility().isFunctionSink();
  }

  @Override
  public TableAnalysis getAnalysis() {
    return function.getFunctionAnalysis();
  }
}
