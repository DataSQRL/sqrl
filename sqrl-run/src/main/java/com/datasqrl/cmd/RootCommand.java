/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

@CommandLine.Command(name = "datasqrl", mixinStandardHelpOptions = true, version = "discover 0.1",
    subcommands = {CompilerCommand.class, RunCommand.class, DiscoverCommand.class, PopulateCommand.class,
        ServeCommand.class})
@Getter
public class RootCommand implements Runnable {

  @CommandLine.Option(names = {"-p", "--package-file"}, description = "Package file"
      , scope = ScopeType.INHERIT)
  protected List<Path> packageFiles = Collections.EMPTY_LIST;

  @Override
  public void run() {
    System.out.println("Choose one of the commands: compile, run, or discover");
  }

  final Path rootDir;

  public RootCommand(Path rootDir) {
    this.rootDir = rootDir;
  }

  public RootCommand() {
    this(Path.of(""));
  }
}
