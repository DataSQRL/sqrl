package com.datasqrl.config;

import com.datasqrl.config.provider.DatabaseConnectionProvider;
import com.datasqrl.config.provider.JDBCConnectionProvider;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.StreamEngine;
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
