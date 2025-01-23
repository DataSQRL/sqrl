package com.datasqrl.flinkwrapper.dag.nodes;


import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import lombok.Getter;
import org.apache.flink.table.catalog.ObjectIdentifier;

@Getter
public abstract class TableNode extends PipelineNode {

  final TableAnalysis tableAnalysis;

  public TableNode(TableAnalysis tableAnalysis) {
    super("table");
    this.tableAnalysis = tableAnalysis;
  }

  public abstract ObjectIdentifier getIdentifier();

  @Override
  public String getId() {
    return getIdentifier().asSummaryString();
  }

}
