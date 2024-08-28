/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.DatabaseHandle;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.apache.commons.lang3.tuple.Triple;

@Getter
@Builder
public class IntegrationTestSettings {
  public enum LogEngine {KAFKA, NONE}
  public enum StreamEngine {FLINK, INMEMORY, NONE}

  public enum DatabaseEngine {INMEMORY, H2, POSTGRES, SQLITE}
  public enum ServerEngine {VERTX}

  @Builder.Default
  final LogEngine log = LogEngine.NONE;
  @Builder.Default
  final StreamEngine stream = StreamEngine.INMEMORY;
  @Builder.Default
  final ServerEngine server = ServerEngine.VERTX;
  @Builder.Default
  final DatabaseEngine database = DatabaseEngine.POSTGRES;


  public static IntegrationTestSettings getInMemory() {
    return IntegrationTestSettings.builder().build();
  }

  public static IntegrationTestSettings getFlinkWithDB() {
    return getEngines(StreamEngine.FLINK, DatabaseEngine.POSTGRES).build();
  }

  public static IntegrationTestSettings.IntegrationTestSettingsBuilder getFlinkWithDBConfig() {
    return getEngines(StreamEngine.FLINK, DatabaseEngine.POSTGRES);
  }

  public static IntegrationTestSettings getFlinkWithDB(DatabaseEngine engine) {
    return getEngines(StreamEngine.FLINK, engine).build();
  }

  public static IntegrationTestSettings.IntegrationTestSettingsBuilder getEngines(StreamEngine stream, DatabaseEngine database) {
    return IntegrationTestSettings.builder().stream(stream).database(database);
  }

  public static IntegrationTestSettings getDatabaseOnly(DatabaseEngine database) {
    return getEngines(StreamEngine.NONE, database).build();
  }

  @Value
  public static class EnginePair {

    DatabaseEngine database;
    StreamEngine stream;

    public String getName() {
      return database.name() + "_" + stream.name();
    }
  }
}
