/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl;

import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.MainScript;
import com.datasqrl.plan.validate.ScriptPlanner;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.junit.jupiter.api.AfterEach;

public class AbstractLogicalSQRLIT extends AbstractEngineIT {

  @AfterEach
  public void tearDown() {
    super.tearDown();
    errors = null;
  }

  protected void plan(String query) {
    ScriptPlanner planner = injector.getInstance(ScriptPlanner.class);
    ModuleLoader moduleLoader = injector.getInstance(ModuleLoader.class);

    planner.plan(new StringMainScript(query), moduleLoader);
  }

  @AllArgsConstructor
  @Getter
  public class StringMainScript implements MainScript {
    final Optional<Path> path = Optional.empty();
    String content;
  }
}
