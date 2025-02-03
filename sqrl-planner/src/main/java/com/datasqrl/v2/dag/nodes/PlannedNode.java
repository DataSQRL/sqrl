package com.datasqrl.v2.dag.nodes;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.plan.global.StageAnalysis;
import java.util.Map;
import lombok.NonNull;
import org.apache.flink.table.catalog.ObjectIdentifier;

public abstract class PlannedNode extends PipelineNode {

  public PlannedNode(@NonNull String type,
      Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    super(type, stageAnalysis);
  }

  public abstract TableAnalysis getAnalysis();

  public ObjectIdentifier getIdentifier() {
    return getAnalysis().getIdentifier();
  }

}
