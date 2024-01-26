/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import picocli.CommandLine;

@CommandLine.Command(name = "compile", description = "Compiles an SQRL script and produces all build artifacts")
public class CompilerCommand extends AbstractCompilerCommand {

  @Override
  public SqrlConfig createSqrlConfig(ErrorCollector errors) {
    return Packager.createDockerConfig(errors);
  }
}
