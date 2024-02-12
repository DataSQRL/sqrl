/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ScriptPlanner;
import java.nio.file.Path;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;

public class AbstractLogicalSQRLIT extends AbstractEngineIT {

  @AfterEach
  public void tearDown() {
    super.tearDown();
    errors = null;
  }

  protected TableSource loadTable(NamePath path, ModuleLoader moduleLoader) {
    TableSourceNamespaceObject ns = (TableSourceNamespaceObject)moduleLoader
        .getModule(path.popLast())
        .get()
        .getNamespaceObject(path.getLast())
        .get();
    return ns.getTable();
  }

  protected void plan(String query) {
    ScriptPlanner planner = injector.getInstance(ScriptPlanner.class);
    planner.plan(new MainScript() {
      @Override
      public Optional<Path> getPath() {
        return Optional.empty();
      }

      @Override
      public String getContent() {
        return query;
      }
    }, moduleLoader);
  }
}
