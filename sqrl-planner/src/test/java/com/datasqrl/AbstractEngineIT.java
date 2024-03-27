/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.inject.StatefulModule;
import com.datasqrl.engine.database.relational.JdbcDataSystemConnector;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.util.DatabaseHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.commons.text.StringSubstitutor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractEngineIT {

  protected DatabaseHandle database = null;

  public SqrlFramework framework;
  public ExecutionPipeline pipeline;
  public ErrorCollector errors;
  public Optional<JdbcDataSystemConnector> jdbc;
  public Injector injector;

  protected void initialize(IntegrationTestSettings settings, Path rootDir,
      Optional<Path> errorDir) {
    initialize(settings, rootDir, errorDir, ErrorCollector.root(), null, false);
  }

  protected void initialize(IntegrationTestSettings settings, Path rootDir, Optional<Path> errorDir,
      ErrorCollector errors, Map<NamePath, SqrlModule> addlModules, boolean isDebug) {
    this.errors = errors;
    Triple<DatabaseHandle, PipelineFactory, SqrlConfig> setup = settings.createSqrlSettings(errors);
    injector = Guice.createInjector(
        new MockSqrlInjector(errors, setup.getRight(), errorDir, rootDir, addlModules, isDebug),
        new StatefulModule(new SqrlSchema(new TypeFactory(), NameCanonicalizer.SYSTEM)));

    this.framework = injector.getInstance(SqrlFramework.class);
    this.database = setup.getLeft();
    this.pipeline = injector.getInstance(ExecutionPipeline.class);
    this.jdbc = pipeline.getStages().stream().filter(f -> f.getEngine() instanceof JDBCEngine)
        .map(f -> ((JDBCEngine) f.getEngine()).getConnector()).findAny();
  }

  protected TableResult executeSql(String flinkSql) {

    Configuration sEnvConfig = Configuration.fromMap(Map.of());
    StreamExecutionEnvironment sEnv = StreamExecutionEnvironment.getExecutionEnvironment(sEnvConfig);

    EnvironmentSettings tEnvConfig = EnvironmentSettings.newInstance()
        .withConfiguration(Configuration.fromMap(Map.of())).build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(sEnv, tEnvConfig);
    StringSubstitutor substitutor = new StringSubstitutor((Map) System.getProperties());

    TableResult tableResult = null;
    for (String sql : flinkSql.split("\n\n")) {
      String replacedSql = substitutor.replace(sql);
      tableResult = tEnv.executeSql(replacedSql);
    }
    return tableResult;
  }

  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.cleanUp();
      database = null;
    }
  }
}
