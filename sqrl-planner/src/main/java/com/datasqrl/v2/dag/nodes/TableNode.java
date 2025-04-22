package com.datasqrl.v2.dag.nodes;


import java.util.Map;
import java.util.Optional;

import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.plan.global.StageAnalysis;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.dag.plan.MutationQuery;

import lombok.Getter;

/**
 * Represents a table in the DAG
 */

@Getter
public class TableNode extends PlannedNode {

  final TableAnalysis tableAnalysis;

  public TableNode(TableAnalysis tableAnalysis, Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    super("table", stageAnalysis);
    this.tableAnalysis = tableAnalysis;
  }

  public boolean isSource() {
    //Table nodes cannot be sinks
    return tableAnalysis.isSourceOrSink();
  }

  public boolean isMutation() {
    return getMutation().isPresent();
  }

  public Optional<MutationQuery> getMutation() {
    if (!isSource()) {
        return Optional.empty();
    }
    return Optional.ofNullable(tableAnalysis.getSourceSinkTable().get().getMutationDefinition());
  }

  @Override
  public TableAnalysis getAnalysis() {
    return tableAnalysis;
  }

  @Override
  public String getId() {
    return getIdentifier().asSummaryString();
  }

}
