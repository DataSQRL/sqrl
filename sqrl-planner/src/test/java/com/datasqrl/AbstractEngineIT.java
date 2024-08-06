/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PipelineFactory;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.inject.StatefulModule;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.util.DatabaseHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.commons.lang3.tuple.Triple;
import org.junit.jupiter.api.AfterEach;

public abstract class AbstractEngineIT {

  protected DatabaseHandle database = null;

  public SqrlFramework framework;
  public ExecutionPipeline pipeline;
  public ErrorCollector errors;
//  public Optional<IJdbcDataSystemConnector> jdbc;
  public Injector injector;

  protected void initialize(IntegrationTestSettings settings, Path rootDir,
      Optional<Path> errorDir) {
    initialize(settings, rootDir, errorDir, ErrorCollector.root(), null, false);
  }

  protected void initialize(IntegrationTestSettings settings, Path rootDir, Optional<Path> errorDir,
      ErrorCollector errors, Map<NamePath, SqrlModule> addlModules, boolean isDebug) {
    this.errors = errors;
    Triple<DatabaseHandle, PipelineFactory, PackageJson> setup = settings.createSqrlSettings(errors);
    injector = Guice.createInjector(
        new MockSqrlInjector(errors, setup.getRight(), errorDir, rootDir, addlModules, isDebug, null),
        new StatefulModule(new SqrlSchema(new TypeFactory(), NameCanonicalizer.SYSTEM)));

    this.framework = injector.getInstance(SqrlFramework.class);
    this.database = setup.getLeft();
    this.pipeline = injector.getInstance(ExecutionPipeline.class);
//    this.jdbc = pipeline.getStages().stream().filter(f -> f.getEngine() instanceof JDBCEngine)
//        .map(f -> ((JDBCEngine) f.getEngine()).getConnector()).findAny();
  }

  @AfterEach
  public void tearDown() {
    if (database != null) {
      database.cleanUp();
      database = null;
    }
  }
}
