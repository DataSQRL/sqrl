package com.datasqrl.flinkwrapper.tables;

import com.datasqrl.engine.database.EngineCreateTable;
import com.datasqrl.engine.log.LogCreateInsertTopic;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.Value;
import org.apache.flink.table.api.Schema;

@Value
public class SourceTableAnalysis {

  /**
   * The connector configuration for the source table
   */
  @NonNull
  FlinkConnectorConfigImpl connector;
  /**
   * The Flink schema of the source table
   */
  @NonNull
  Schema schema;
  /**
   * This is set for internal CREATE TABLE definitions that map to mutations only, otherwise null
   * It contains the metadata information from the log engine on where to write the data
   */
  @Nullable
  LogCreateInsertTopic mutationDefinition;
}
