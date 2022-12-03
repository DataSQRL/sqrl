package ai.datasqrl.config;

import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.metadata.MetadataStoreProvider;
import ai.datasqrl.physical.pipeline.ExecutionPipeline;
import ai.datasqrl.physical.stream.StreamEngine;
import lombok.NonNull;
import lombok.Value;

@Value
/**
 * TODO: need to add proper error handling
 */
public class EngineSettings {

  @NonNull
  ExecutionPipeline pipeline;
  @NonNull
  MetadataStoreProvider metadataStoreProvider;
  @NonNull
  DatabaseConnectionProvider database;
  @NonNull
  StreamEngine stream;

  public JDBCConnectionProvider getJDBC() {
    return (JDBCConnectionProvider) getDatabase();
  }





}
