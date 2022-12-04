/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Runs an SQRL script")
public class RunCommand extends AbstractCompilerCommand {

  protected RunCommand() {
    super(true);
  }

}
