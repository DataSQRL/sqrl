/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.config.GlobalEngineConfiguration;
import com.datasqrl.engine.EngineConfiguration;
import com.datasqrl.engine.database.inmemory.InMemoryDatabaseConfiguration;
import com.datasqrl.engine.database.inmemory.InMemoryMetadataStore;
import com.datasqrl.engine.stream.flink.FlinkEngineConfiguration;
import com.datasqrl.engine.stream.inmemory.InMemoryStreamConfiguration;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.local.generate.DebuggerConfig;
import com.datasqrl.util.DatabaseHandle;
import com.datasqrl.util.JDBCTestDatabase;
import java.util.ArrayList;
import java.util.List;
import lombok.Builder;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

@Value
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

  Pair<DatabaseHandle, EngineSettings> getSqrlSettings() {

    List<EngineConfiguration> engines = new ArrayList<>();
    //Stream engine
    switch (getStream()) {
      case FLINK:
        engines.add(new FlinkEngineConfiguration());
        break;
      case INMEMORY:
        engines.add(new InMemoryStreamConfiguration());
        break;
    }
    //Database engine
    DatabaseHandle database = null;
    switch (getDatabase()) {
      case INMEMORY:
        engines.add(new InMemoryDatabaseConfiguration());
        database = () -> InMemoryMetadataStore.clearLocal();
        break;
      case H2:
      case POSTGRES:
      case SQLITE:
        JDBCTestDatabase jdbcDB = new JDBCTestDatabase(getDatabase());
        engines.add(jdbcDB.getJdbcConfiguration());
        database = jdbcDB;
        break;
      default:
        throw new RuntimeException("Could not find db engine");
    }
    GlobalEngineConfiguration engineConfig = GlobalEngineConfiguration.builder().engines(engines)
        .build();
    ErrorCollector errors = ErrorCollector.root();
    EngineSettings engineSettings = engineConfig.initializeEngines(errors);
    assertNotNull(engineSettings, errors.toString());
    return Pair.of(database, engineSettings);
  }

  public static IntegrationTestSettings getInMemory() {
    return IntegrationTestSettings.builder().build();
  }

  public static IntegrationTestSettings getFlinkWithDB() {
    return getEngines(StreamEngine.FLINK, DatabaseEngine.H2).build();
  }

  public static IntegrationTestSettings.IntegrationTestSettingsBuilder getFlinkWithDBConfig() {
    return getEngines(StreamEngine.FLINK, DatabaseEngine.H2);
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
