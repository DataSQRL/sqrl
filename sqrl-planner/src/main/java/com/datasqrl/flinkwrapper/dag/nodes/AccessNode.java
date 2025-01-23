package com.datasqrl.flinkwrapper.dag.nodes;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.flinkwrapper.tables.AnnotatedSqrlTableFunction;
import com.datasqrl.flinkwrapper.tables.SqrlTableFunction;
import com.datasqrl.plan.global.StageAnalysis;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
public class AccessNode extends PipelineNode {

  private final AnnotatedSqrlTableFunction function;

  protected AccessNode(AnnotatedSqrlTableFunction function, Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    super("access", stageAnalysis);
    this.function = function;
  }

  @Override
  public String getId() {
    return "";
  }

  @Override
  public boolean isSink() {
    return true;
  }
}
