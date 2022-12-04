package com.datasqrl.cmd;

import picocli.CommandLine;

@CommandLine.Command(name = "run", description = "Runs an SQRL script")
public class RunCommand extends AbstractCompilerCommand {

  protected RunCommand() {
    super(true);
  }

}
