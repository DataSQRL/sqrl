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



  public Triple<DatabaseHandle, PipelineFactory, PackageJson> createSqrlSettings(
      ErrorCollector errors) {
//    SqrlConfig config = SqrlConfig.createCurrentVersion(errors);
//    SqrlConfig compilerConfig = config.getSubConfig(COMPILER_KEY);
//
//    compilerConfig.setProperty("errorSink", errorSink.getDisplay());
//
//
//    SqrlConfig engineConfig = config.getSubConfig(ENGINES_PROPERTY);
//    //Stream engine
//    String streamEngineName = null;
//    switch (getStream()) {
//      case FLINK:
//        streamEngineName = FlinkEngineFactory.ENGINE_NAME;
//        break;
//      case INMEMORY:
//        throw new UnsupportedOperationException("Not supported anymore");
//    }
//    if (!Strings.isNullOrEmpty(streamEngineName)) {
//      engineConfig.getSubConfig("streams")
//          .setProperty(EngineFactory.ENGINE_NAME_KEY, streamEngineName);
//    }
//
//    //Flink config
//    if (getStream() == StreamEngine.FLINK) {
//      SqrlConfig stream = engineConfig.getSubConfig("streams");
//
//      if (System.getProperty("os.name").toLowerCase().contains("mac")) {
//        stream.setProperty(TaskManagerOptions.NETWORK_MEMORY_MIN.key(), "256mb");
//        stream.setProperty(TaskManagerOptions.NETWORK_MEMORY_MAX.key(), "256mb");
//        stream.setProperty(TaskManagerOptions.MANAGED_MEMORY_SIZE.key(), "256mb");
//      }
//    }
//
//    if (getServer() == ServerEngine.VERTX) {
//      SqrlConfig server = engineConfig.getSubConfig("server");
//      server.setProperty(EngineFactory.ENGINE_NAME_KEY, "vertx");
//    }
//
//    //Database engine
//    DatabaseHandle database = null;
//    SqrlConfig dbconfig = engineConfig.getSubConfig("database");
//    switch (getDatabase()) {
//      case INMEMORY:
//        throw new UnsupportedOperationException("Not supported anymore");
//      case H2:
//      case POSTGRES:
//      case SQLITE:
//        JDBCTestDatabase jdbcDB = new JDBCTestDatabase(getDatabase());
//        dbconfig.setProperty(JDBCEngineFactory.ENGINE_NAME_KEY, JDBCEngineFactory.ENGINE_NAME);
//        dbconfig.copy(jdbcDB.getConnector().toFlinkConnector());
//        database = jdbcDB;
//        break;
//      default:
//        throw new RuntimeException("Could not find db engine");
//    }
//
//    if (getLog() == LogEngine.KAFKA) {
//      SqrlConfig log = engineConfig.getSubConfig("log");
//      log.setProperty(KafkaLogEngineFactory.ENGINE_NAME_KEY, KafkaLogEngineFactory.ENGINE_NAME);
//      log.setProperty("type", ExternalDataType.source_and_sink.name());
//      SqrlConfig connector = log.getSubConfig("connector");
//      connector.setProperty(KafkaLogEngineFactory.ENGINE_NAME_KEY,
//          KafkaLogEngineFactory.ENGINE_NAME);
//      log.getSubConfig("format")
//          .setProperty("name", "json");
//      log.setProperty("schema", "flexible");
//    }


//    return Triple.of(database, null, config);
    throw new RuntimeException();
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
