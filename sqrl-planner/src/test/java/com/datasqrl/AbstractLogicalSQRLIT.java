/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.TableLoader;
import com.datasqrl.name.NamePath;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.calcite.Planner;
import com.datasqrl.plan.calcite.PlannerFactory;
import com.datasqrl.plan.local.generate.Resolve;
import com.datasqrl.plan.local.generate.Session;
import java.nio.file.Files;
import java.nio.file.Path;
import lombok.SneakyThrows;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.SqrlCalciteSchema;
import org.apache.calcite.sql.ScriptNode;
import org.junit.jupiter.api.AfterEach;

public class AbstractLogicalSQRLIT extends AbstractEngineIT {

  @AfterEach
  public void tearDown() {
    super.tearDown();
    error = null;

  }

  public ErrorCollector error;
  public SqrlParser parser;
  public Planner planner;
  public Resolve resolve;
  public Session session;
  public Path rootDir;


  protected void initialize(IntegrationTestSettings settings, Path rootDir) {
    super.initialize(settings);
    error = ErrorCollector.root();

    planner = new PlannerFactory(
        new SqrlCalciteSchema(
            CalciteSchema.createRootSchema(false, false).plus()).plus()).createPlanner();
    Session session = new Session(error, planner, engineSettings.getPipeline(), settings.getDebugger());
    this.session = session;
    this.parser = new SqrlParser();
    this.resolve = new Resolve(rootDir);
    this.rootDir = rootDir;
  }

  protected TableSource loadTable(NamePath path) {
    TableLoader loader = new TableLoader();
    return loader.readTable(rootDir, path, error).get();
  }

  @SneakyThrows
  protected String loadScript(String name) {
    Path path = rootDir.resolve(name);
    return Files.readString(path);
  }

  protected Resolve.Env plan(String script) {
    ErrorCollector scriptError = error.withFile("test.sqrl", script);
    ScriptNode node = parser.parse(script, scriptError);
    return resolve.planDag(session, node, scriptError);
  }

}
