/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import lombok.Getter;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

@CommandLine.Command(name = "datasqrl", mixinStandardHelpOptions = true, version = "discover 0.1",
    subcommands = {CompilerCommand.class, RunCommand.class, DiscoverCommand.class, PopulateCommand.class,
        ServeCommand.class, PublishCommand.class})
@Getter
public class RootCommand implements Runnable {

  @CommandLine.Option(names = {"-c", "--config"}, description = "Package configuration file(s)"
      , scope = ScopeType.INHERIT)
  protected List<Path> packageFiles = Collections.EMPTY_LIST;

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }

  final Path rootDir;

  public RootCommand(Path rootDir) {
    this.rootDir = rootDir;
  }

  public RootCommand() {
    this(Path.of(""));
  }

  public CommandLine getCmd() {
    return new CommandLine(this).setCaseInsensitiveEnumValuesAllowed(true);
  }
}
