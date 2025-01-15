package com.datasqrl.flinkwrapper.dag.nodes;

import com.datasqrl.engine.log.LogEngine;
import com.datasqrl.flinkwrapper.tables.FlinkTableConfig;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.flink.table.catalog.ObjectIdentifier;

@AllArgsConstructor
public class ImportTableNode extends TableNode {

  private final FlinkTableConfig tableDefinition;
  /**
   * If this is a mutation, this is the log engine managing the mutation event stream
   */
  private final Optional<LogEngine> logEngine;

  public boolean isMutation() {
    return logEngine.isPresent();
  }

  @Override
  public ObjectIdentifier getIdentifier() {
    return null;
  }
}
