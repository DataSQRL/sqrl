package com.datasqrl.flinkwrapper.dag.nodes;

import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.flinkwrapper.analyzer.cost.SimpleCostAnalysisModel;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.plan.global.StageAnalysis.Cost;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.flink.table.catalog.ObjectIdentifier;

@Getter
public class ExportNode extends PipelineNode {

  private final String sinkId;

  private final Optional<ExecutionStage> sinkTo;
  private final Optional<ObjectIdentifier> createdSinkTable;


  public ExportNode(Map<ExecutionStage, StageAnalysis> stageAnalysis,
      String sinkId, Optional<ExecutionStage> sinkTo, Optional<ObjectIdentifier> createdSinkTable) {
    super("export", stageAnalysis);
    this.sinkId = sinkId;
    this.sinkTo = sinkTo;
    this.createdSinkTable = createdSinkTable;
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
