/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.SqrlPlan;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.analyze.RetailSqrlModule;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.SqrlQueryPlanner;
import com.datasqrl.util.DatabaseHandle;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;

public class AbstractLogicalSQRLIT extends AbstractEngineIT {

  @AfterEach
  public void tearDown() {
    super.tearDown();
    errors = null;

  }

  public ErrorCollector errors;
  public SqrlQueryPlanner planner;
  public Path rootDir;
  private SqrlPlan sqrlPlanner;

  protected void initialize(IntegrationTestSettings settings, Path rootDir) {
    initialize(settings, rootDir, Optional.empty());
  }
  protected void initialize(IntegrationTestSettings settings, Path rootDir, Optional<Path> errorDir) {
    Map<NamePath, SqrlModule> addlModules = Map.of();
    if (rootDir == null) {
      addlModules = Map.of(NamePath.of("ecommerce-data"), new RetailSqrlModule());
    }
    Pair<DatabaseHandle, EngineSettings> engines = settings.getSqrlSettings();
    this.engineSettings = engines.getRight();
    this.database = engines.getLeft();
    SqrlTestDIModule module = new SqrlTestDIModule(engineSettings.getPipeline(), settings, rootDir, addlModules, errorDir,
        ErrorCollector.root());
    Injector injector = Guice.createInjector(module);
    initialize(settings, rootDir, injector);
  }

  protected void initialize(IntegrationTestSettings settings, Path rootDir, Injector injector) {
    super.initialize(settings, injector);

    errors = injector.getInstance(ErrorCollector.class);
    planner = injector.getInstance(SqrlQueryPlanner.class);
    sqrlPlanner = injector.getInstance(SqrlPlan.class);
  }

  protected TableSource loadTable(NamePath path) {
    TableSourceNamespaceObject ns = (TableSourceNamespaceObject)injector.getInstance(ModuleLoader.class)
        .getModule(path.popLast())
        .get()
        .getNamespaceObject(path.getLast())
        .get();
    return ns.getTable();
  }

  protected Namespace plan(String query) {
    return sqrlPlanner.plan(query);
  }

}
