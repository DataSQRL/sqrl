/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "execute", description = "Runs an SQRL script")
public class ExecuteCommand extends AbstractCompilerCommand {

  protected ExecuteCommand() {
    super(true, false);
  }

}
