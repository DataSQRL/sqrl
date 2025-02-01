package com.datasqrl.flinkwrapper.dag.nodes;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
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
