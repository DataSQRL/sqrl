package com.datasqrl.flinkwrapper.dag.nodes;


import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.log.LogCreateInsertTopic;
import com.datasqrl.engine.pipeline.ExecutionStage;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.plan.global.StageAnalysis;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.flink.table.catalog.ObjectIdentifier;

@Getter
public class TableNode extends PlannedNode {

  final TableAnalysis tableAnalysis;

  public TableNode(TableAnalysis tableAnalysis, Map<ExecutionStage, StageAnalysis> stageAnalysis) {
    super("table", stageAnalysis);
    this.tableAnalysis = tableAnalysis;
  }

  public boolean isSource() {
    return tableAnalysis.isSource();
  }

  public boolean isMutation() {
    return getMutation().isPresent();
  }

  public Optional<LogCreateInsertTopic> getMutation() {
    if (!isSource()) return Optional.empty();
    return Optional.ofNullable(tableAnalysis.getSourceTable().get().getMutationDefinition());
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
