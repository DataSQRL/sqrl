package com.datasqrl.flinkwrapper.dag.nodes;

import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.flinkwrapper.analyzer.TableAnalysis;
import com.datasqrl.flinkwrapper.tables.FlinkTableConfig;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.flink.table.catalog.ObjectIdentifier;

public class ImportTableNode extends TableNode {

  /**
   * If this is a mutation, this is the log engine managing the mutation event stream
   */
  final Optional<LogEngine> logEngine;

  public ImportTableNode(TableAnalysis tableAnalysis, Optional<LogEngine> logEngine) {
    super(tableAnalysis);
    this.logEngine = logEngine;
    assert tableAnalysis.isSource() : tableAnalysis;
  }

  public boolean isMutation() {
    return logEngine.isPresent();
  }

  @Override
  public ObjectIdentifier getIdentifier() {
    return null;
  }
}
