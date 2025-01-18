package com.datasqrl.flinkwrapper.dag.nodes;

import lombok.Getter;

@Getter
public abstract class ExportNode extends PipelineNode {

  private final String sinkId;

  protected ExportNode(String sinkId) {
    super("export");
    this.sinkId = sinkId;
  }

  @Override
  public boolean isSink() {
    return true;
  }

  @Override
  public String getId() {
    return sinkId;
  }

}
