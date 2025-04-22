package com.datasqrl.v2.dag.nodes;

import java.util.Map;
import java.util.Optional;

import org.apache.flink.table.catalog.ObjectIdentifier;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;

import lombok.Getter;

/**
 * Represents an EXPORT statement in the DAG
 */
@Getter
public class ExportNode extends PipelineNode {

  private final NamePath sinkPath;

  private final Optional<ExecutionStage> sinkTo;
  private final Optional<ObjectIdentifier> createdSinkTable;


  public ExportNode(Map<ExecutionStage, StageAnalysis> stageAnalysis,
      NamePath sinkPath, Optional<ExecutionStage> sinkTo, Optional<ObjectIdentifier> createdSinkTable) {
    super("export", stageAnalysis);
    this.sinkPath = sinkPath;
    this.sinkTo = sinkTo;
    this.createdSinkTable = createdSinkTable;
  }

  @Override
  public boolean isSink() {
    return true;
  }

  @Override
  public String getId() {
    return sinkPath.toString();
  }

}
