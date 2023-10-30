/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import com.datasqrl.compile.Compiler.CompilerResult;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import com.datasqrl.service.PackagerUtil;
import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Compiles a SQRL script and runs the entire generated data pipeline")
public class RunCommand extends AbstractCompilerCommand {

  protected RunCommand() {
    super(true, true, true);
  }

  @Override
  protected SqrlConfig initializeConfig(DefaultConfigSupplier configSupplier, ErrorCollector errors) {
    return getDefaultConfig(true, errors)
        .get();
  }


  @Override
  protected void postCompileActions(DefaultConfigSupplier configSupplier, Packager packager,
      CompilerResult result, ErrorCollector errors) {
    super.postCompileActions(configSupplier, packager, result, errors);

    executePlan(result.getPlan(), errors);
  }
}
