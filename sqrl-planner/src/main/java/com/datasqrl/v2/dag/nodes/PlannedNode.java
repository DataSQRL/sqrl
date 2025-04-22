package com.datasqrl.v2.dag.nodes;

import java.util.Map;

import org.apache.flink.table.catalog.ObjectIdentifier;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.v2.analyzer.TableAnalysis;

import lombok.NonNull;

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
