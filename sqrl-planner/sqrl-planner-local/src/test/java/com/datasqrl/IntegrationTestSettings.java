/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.EngineFactory;
import com.datasqrl.engine.database.inmemory.InMemoryDatabaseFactory;
import com.datasqrl.engine.database.inmemory.InMemoryMetadataStore;
import com.datasqrl.engine.database.relational.JDBCEngineFactory;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.engine.stream.inmemory.InMemoryStreamFactory;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.util.DatabaseHandle;
import com.datasqrl.util.JDBCTestDatabase;
import com.google.common.base.Strings;
import lombok.Builder;
import lombok.Getter;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.configuration.TaskManagerOptions;

@Getter
@Builder
public class IntegrationTestSettings {
  public enum StreamEngine {FLINK, INMEMORY}

  public enum DatabaseEngine {INMEMORY, H2, POSTGRES, SQLITE}

  @Builder.Default
  final StreamEngine stream = StreamEngine.INMEMORY;
  @Builder.Default
  final DatabaseEngine database = DatabaseEngine.INMEMORY;
  @Builder.Default
  final DebuggerConfig debugger = DebuggerConfig.NONE;
  @Builder.Default
  final NamePath errorSink = NamePath.of("print","errors");


  public Pair<DatabaseHandle, PipelineFactory> getSqrlSettings() {
    ErrorCollector errors = ErrorCollector.root();
    SqrlConfig config = SqrlConfigCommons.create(errors);

    //Stream engine
    String streamEngineName = null;
    switch (getStream()) {
      case FLINK:
        streamEngineName = FlinkEngineFactory.ENGINE_NAME;
        break;
      case INMEMORY:
        streamEngineName = InMemoryStreamFactory.ENGINE_NAME;
        break;
    }
    if (!Strings.isNullOrEmpty(streamEngineName)) {
      config.getSubConfig("streams")
          .setProperty(EngineFactory.ENGINE_NAME_KEY, streamEngineName);
    }

    //Flink config
    if (getStream() == StreamEngine.FLINK) {
      SqrlConfig stream = config.getSubConfig("streams");

      if (System.getProperty("os.name").toLowerCase().contains("mac")) {
        stream.setProperty(TaskManagerOptions.NETWORK_MEMORY_MIN.key(), "256mb");
        stream.setProperty(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "256mb");
        stream.setProperty(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "256mb");
      }
    }

    //Database engine
    DatabaseHandle database = null;
    SqrlConfig dbconfig = config.getSubConfig("database");
    switch (getDatabase()) {
      case INMEMORY:
        dbconfig.setProperty(InMemoryDatabaseFactory.ENGINE_NAME_KEY, InMemoryDatabaseFactory.ENGINE_NAME);
        database = () -> InMemoryMetadataStore.clearLocal();
        break;
      case H2:
      case POSTGRES:
      case SQLITE:
        JDBCTestDatabase jdbcDB = new JDBCTestDatabase(getDatabase());
        dbconfig.setProperty(JDBCEngineFactory.ENGINE_NAME_KEY, JDBCEngineFactory.ENGINE_NAME);
        dbconfig.setProperties(jdbcDB.getConnector());
        database = jdbcDB;
        break;
      default:
        throw new RuntimeException("Could not find db engine");
    }

    PipelineFactory pipelineFactory = new PipelineFactory(config);
    return Pair.of(database, pipelineFactory);
  }

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
    return getEngines(IntegrationTestSettings.StreamEngine.INMEMORY, database).build();
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
