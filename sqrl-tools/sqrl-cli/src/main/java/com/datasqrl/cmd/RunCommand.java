/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Compiles a SQRL script and runs the entire generated data pipeline")
public class RunCommand extends AbstractCompilerCommand {

  protected RunCommand() {
    super(true, true, true);
  }

}
