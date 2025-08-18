/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.cli;

import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import lombok.Getter;
import lombok.NonNull;
import picocli.CommandLine;
import picocli.CommandLine.ScopeType;

@CommandLine.Command(
    name = "datasqrl",
    mixinStandardHelpOptions = true,
    versionProvider = CliVersionProvider.class,
    subcommands = {CompileCmd.class, TestCmd.class, RunCmd.class, ExecuteCmd.class})
@Getter
public class DatasqrlCli implements Runnable {

  @CommandLine.Option(
      names = {"-c", "--config"},
      description = "Package configuration file(s)",
      scope = ScopeType.INHERIT)
  protected List<Path> packageFiles = Collections.emptyList();

  @Override
  public void run() {
    CommandLine.usage(this, System.out);
  }

  final Path rootDir;
  final StatusHook statusHook;
  final boolean internalTestExec;

  public DatasqrlCli(
      @NonNull Path rootDir, @NonNull StatusHook statusHook, boolean internalTestExec) {
    this.rootDir = rootDir;
    this.statusHook = statusHook;
    this.internalTestExec = internalTestExec;
  }

  public DatasqrlCli(@NonNull Path rootDir) {
    this(rootDir, StatusHook.NONE, false);
  }

  public DatasqrlCli() {
    this(Path.of("").toAbsolutePath());
  }

  public CommandLine getCmd() {
    return new CommandLine(this).setCaseInsensitiveEnumValuesAllowed(true);
  }

  public static void main(String[] args) {
    var cli = new DatasqrlCli();
    var exitCode = cli.getCmd().execute(args);
    exitCode += (cli.getStatusHook().isSuccess() ? 0 : 1);

    System.exit(exitCode);
  }
}
