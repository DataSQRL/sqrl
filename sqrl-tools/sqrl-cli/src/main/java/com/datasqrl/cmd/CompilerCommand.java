/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "compile", description = "Compiles an SQRL script and produces all build artifacts")
public class CompilerCommand extends AbstractCompilerCommand {

  protected CompilerCommand() {
    super(false, false, false);
  }

}
