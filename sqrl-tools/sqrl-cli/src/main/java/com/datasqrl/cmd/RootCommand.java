/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.cmd;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

@CommandLine.Command(name = "datasqrl", mixinStandardHelpOptions = true, version = "v0.5.10",
    subcommands = {CompilerCommand.class, TestCommand.class, LoginCommand.class, RunCommand.class})
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
  final StatusHook statusHook;

  public RootCommand(@NonNull Path rootDir, @NonNull StatusHook statusHook) {
    this.rootDir = rootDir;
    this.statusHook = statusHook;
  }

  public RootCommand(@NonNull Path rootDir) {
    this(rootDir, StatusHook.NONE);
  }

  public RootCommand() {
    this(Path.of(""));
  }

  public CommandLine getCmd() {
    return new CommandLine(this).setCaseInsensitiveEnumValuesAllowed(true);
  }

}
