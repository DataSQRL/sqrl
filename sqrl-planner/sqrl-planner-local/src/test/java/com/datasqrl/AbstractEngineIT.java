/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import static com.datasqrl.loaders.LoaderUtil.loadSink;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.engine.database.relational.JDBCEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.inject.MockSqrlInjector;
import com.datasqrl.inject.StatefulModule;
import com.datasqrl.io.impl.jdbc.JdbcDataSystemConnector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleLoaderImpl;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.plan.hints.SqrlHintStrategyTable;
import com.datasqrl.plan.local.analyze.MockModuleLoader;
import com.datasqrl.plan.local.generate.Debugger;
import com.datasqrl.plan.rules.SqrlRelMetadataProvider;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.TableConverter;
import com.datasqrl.plan.table.TableIdFactory;
import com.datasqrl.util.DatabaseHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.nio.file.Path;
import java.util.HashMap;
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

  public DatabaseHandle database = null;

  public SqrlFramework framework;
  public IntegrationTestSettings settings;
  public ExecutionPipeline pipeline;
  public ErrorCollector errors;
  public NameCanonicalizer nameCanonicalizer;
  public Path rootDir;
  public Optional<Path> errorDir;
  public ModuleLoader moduleLoader;
  public ErrorSink errorSink;
  public Optional<JdbcDataSystemConnector> jdbc;
  public PipelineFactory pipelineFactory;
  public Debugger debugger;
  public Injector injector;

  protected void initialize(IntegrationTestSettings settings, Path rootDir, Optional<Path> errorDir) {
    ModuleLoader loader;
    this.errors = ErrorCollector.root();
    if (rootDir != null) {
      loader = new ModuleLoaderImpl(new ObjectLoaderImpl(new FileResourceResolver(rootDir),
          errors, new CalciteTableFactory(new TableIdFactory(new HashMap<>()), new TableConverter(new TypeFactory(),
          NameCanonicalizer.SYSTEM))));
    } else {
      loader = new MockModuleLoader(null, Map.of(), errorDir);
    }
    initialize(settings, rootDir, errorDir, loader, errors);
  }

  protected void initialize(IntegrationTestSettings settings, Path rootDir, Optional<Path> errorDir,
      ModuleLoader moduleLoader, ErrorCollector errors) {
    this.errors = errors;

    Triple<DatabaseHandle, PipelineFactory, SqrlConfig> setup = settings.createSqrlSettings(errors);

    injector = Guice.createInjector(
        new MockSqrlInjector(moduleLoader, errors, setup.getRight()),
        new StatefulModule(new SqrlSchema(new TypeFactory(), NameCanonicalizer.SYSTEM)));

    this.framework = injector.getInstance(SqrlFramework.class);

    Map<NamePath, SqrlModule> addlModules = (rootDir == null)
        ? TestModuleFactory.merge(TestModuleFactory.createRetail(framework),
        TestModuleFactory.createFuzz(framework))
        : Map.of();

    this.moduleLoader = createModuleLoader(rootDir, addlModules, errors,
        errorDir, new CalciteTableFactory(new TableIdFactory(framework.getSchema().getTableNameToIdMap()),
            new TableConverter(framework.getTypeFactory(),
            framework.getNameCanonicalizer())));
    this.nameCanonicalizer = NameCanonicalizer.SYSTEM;
    this.errorSink = new ErrorSink(loadSink(settings.getErrorSink(), errors, moduleLoader));

    this.settings = settings;
    this.database = setup.getLeft();
    this.pipelineFactory = injector.getInstance(PipelineFactory.class);
    this.pipeline = injector.getInstance(ExecutionPipeline.class);
    this.rootDir = rootDir;
    this.errorDir = errorDir;
    this.jdbc = pipeline.getStages().stream()
        .filter(f->f.getEngine() instanceof JDBCEngine)
        .map(f->((JDBCEngine) f.getEngine()).getConnector())
        .findAny();
    this.debugger = new Debugger(settings.debugger, moduleLoader);
  }

  public static ModuleLoader createModuleLoader(Path rootDir, Map<NamePath, SqrlModule> addlModules,
      ErrorCollector errors, Optional<Path> errorDir, CalciteTableFactory tableFactory) {
    if (rootDir != null) {
      ObjectLoaderImpl objectLoader = new ObjectLoaderImpl(new FileResourceResolver(rootDir),
          errors, tableFactory);
      return new MockModuleLoader(objectLoader, addlModules, errorDir);
    } else {
      return new MockModuleLoader(null, addlModules, errorDir);
    }
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
