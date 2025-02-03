package com.datasqrl.v2.tables;

import com.datasqrl.v2.dag.plan.MutationQuery;
import javax.annotation.Nullable;
import lombok.NonNull;
import lombok.Value;
import org.apache.flink.table.api.Schema;

/**
 * Metadata we keep track off for imported tables
 */
@Value
public class SourceTableAnalysis {

  /**
   * The connector configuration for the source table
   */
  @NonNull
  FlinkConnectorConfig connector;
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
  MutationQuery mutationDefinition;
}
