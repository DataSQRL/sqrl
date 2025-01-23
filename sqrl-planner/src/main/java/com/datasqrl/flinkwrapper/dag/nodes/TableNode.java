package com.datasqrl.flinkwrapper.dag.nodes;


import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.flinkwrapper.tables.SourceTableAnalysis;
import com.datasqrl.plan.global.StageAnalysis;
import com.google.common.base.Preconditions;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.flink.table.catalog.ObjectIdentifier;

@Getter
public class TableNode extends PipelineNode {

  final TableAnalysis tableAnalysis;

  public TableNode(TableAnalysis tableAnalysis, Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    super("table", stageAnalysis);
    this.tableAnalysis = tableAnalysis;
  }

  public boolean isSource() {
    return tableAnalysis.isSource();
  }

  public boolean isMutation() {
    if (!isSource()) return false;
    return tableAnalysis.getSourceTable().get().getLogMetadata()!=null;
  }

  public ObjectIdentifier getIdentifier() {
    return tableAnalysis.getIdentifier();
  }

  @Override
  public String getId() {
    return getIdentifier().asSummaryString();
  }

}
