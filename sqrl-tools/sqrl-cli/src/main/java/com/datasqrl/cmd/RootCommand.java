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
package com.datasqrl.cmd;

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
    version = "v0.5.10",
    subcommands = {CompilerCommand.class, TestCommand.class, RunCommand.class})
@Getter
public class RootCommand implements Runnable {

  @CommandLine.Option(
      names = {"-c", "--config"},
      description = "Package configuration file(s)",
      scope = ScopeType.INHERIT)
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
