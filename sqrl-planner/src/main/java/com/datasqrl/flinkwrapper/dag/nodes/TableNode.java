package com.datasqrl.flinkwrapper.dag.nodes;


import org.apache.flink.table.catalog.ObjectIdentifier;

public abstract class TableNode extends PipelineNode {

  public TableNode() {
    super("table");
  }

  public abstract ObjectIdentifier getIdentifier();

  @Override
  public String getId() {
    return getIdentifier().asSummaryString();
  }
}
