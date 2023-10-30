/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import picocli.CommandLine;

@CommandLine.Command(name = "validate", description = "Validates a SQRL script")
public class ValidateCommand extends AbstractCompilerCommand {

  protected ValidateCommand() {
    super(false, false, false);
  }

  @Override
  protected SqrlConfig initializeConfig(DefaultConfigSupplier configSupplier, ErrorCollector errors) {
    return getDefaultConfig(false, errors)
        .get();
  }


  @Override
  protected void postCompileActions(DefaultConfigSupplier configSupplier, Packager packager,
      CompilerResult result, ErrorCollector errors) {
  }
}
