package com.datasqrl.flinkwrapper.dag.nodes;

public class AccessNode extends PipelineNode {

  protected AccessNode() {
    super("query");
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
