package com.datasqrl.flinkwrapper.dag.nodes;

public class QueryNode extends PipelineNode {

  protected QueryNode() {
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
