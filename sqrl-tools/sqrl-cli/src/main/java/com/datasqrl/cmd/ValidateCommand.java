/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import java.nio.file.Path;
import picocli.CommandLine;

@CommandLine.Command(name = "validate", description = "Validates a SQRL script")
public class ValidateCommand extends AbstractCompilerCommand {

  @Override
  public SqrlConfig createDefaultConfig(ErrorCollector errors) {
    return Packager.createDockerConfig(errors);
  }

  @Override
  protected void postprocess(Packager packager, Path targetDir,
      ErrorCollector errors) {
  }
}
