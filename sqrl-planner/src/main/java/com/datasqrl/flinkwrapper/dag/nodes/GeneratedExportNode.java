package com.datasqrl.flinkwrapper.dag.nodes;

import com.datasqrl.engine.pipeline.ExecutionStage;
import lombok.Getter;

@Getter
public class GeneratedExportNode extends ExportNode {

  private final ExecutionStage stage;
  private final String name;

  public GeneratedExportNode(String sinkId, ExecutionStage stage, String name) {
    super(sinkId);
    this.stage = stage;
    this.name = name;
  }

}
