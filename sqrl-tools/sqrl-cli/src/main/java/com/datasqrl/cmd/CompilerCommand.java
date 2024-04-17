/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.config.PackageJson;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ExecutionGoal;
import picocli.CommandLine;

@CommandLine.Command(name = "compile", description = "Compiles an SQRL script and produces all build artifacts")
public class CompilerCommand extends AbstractCompilerCommand {

  @Override
  public PackageJson createDefaultConfig(ErrorCollector errors) {
    throw new RuntimeException("package.json required");
//    return Packager.createDockerConfig(errors);
  }

  @Override
  public ExecutionGoal getGoal() {
    return ExecutionGoal.COMPILE;
  }
}
