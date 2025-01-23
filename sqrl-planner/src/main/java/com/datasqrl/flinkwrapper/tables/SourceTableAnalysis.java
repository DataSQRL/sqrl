package com.datasqrl.flinkwrapper.tables;

import lombok.Value;
import org.apache.flink.table.api.Schema;

@Value
public class SourceTableAnalysis {

  FlinkConnectorConfigImpl connector;
  Schema schema;
  LogEngineTableMetadata logMetadata;
}
