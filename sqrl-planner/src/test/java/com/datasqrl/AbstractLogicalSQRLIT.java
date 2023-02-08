/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.URLObjectLoaderImpl;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.name.NamePath;
import com.datasqrl.parse.SqrlParser;
import com.datasqrl.plan.local.generate.FileResourceResolver;
import com.datasqrl.plan.local.generate.Namespace;
import com.datasqrl.plan.local.generate.Resolve;
import com.datasqrl.plan.local.generate.Session;
import com.google.common.base.Preconditions;
import java.net.URL;
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
  public Resolve resolve;
  public Session session;
  public Path rootDir;


  protected void initialize(IntegrationTestSettings settings, Path rootDir) {
    super.initialize(settings);
    error = ErrorCollector.root();

    SqrlCalciteSchema schema =
        new SqrlCalciteSchema(
            CalciteSchema.createRootSchema(false, false).plus());
    this.session = Session.createSession(error, engineSettings.getPipeline(),
        settings.getDebugger(), schema);
    this.parser = new SqrlParser();
    this.resolve = new Resolve(rootDir);
    Preconditions.checkState(rootDir.toFile().exists(), "Root dir does not exist");
    this.rootDir = rootDir;
  }

  protected TableSource loadTable(NamePath path) {
    URLObjectLoaderImpl URLObjectLoaderImpl = new URLObjectLoaderImpl(error);
    FileResourceResolver fileResourceResolver = new FileResourceResolver(rootDir);
    URL url = fileResourceResolver.resolveTableJson(path)
        .get();

    TableSourceNamespaceObject ts = (TableSourceNamespaceObject) URLObjectLoaderImpl.load(url, fileResourceResolver, path)
        .get();
    return ts.getTable();
  }

  @SneakyThrows
  protected String loadScript(String name) {
    Path path = rootDir.resolve(name);
    return Files.readString(path);
  }

  protected Namespace plan(String script) {
    ErrorCollector scriptError = error.withFile("test.sqrl", script);
    ScriptNode node = parser.parse(script, scriptError);
    return resolve.planDag(node, scriptError, new FileResourceResolver(rootDir),
        session);
  }

}
