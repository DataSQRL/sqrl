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
package com.datasqrl.intellij;

import com.datasqrl.intellij.SqrlSettings.Launcher;
import com.google.gson.JsonPrimitive;
import com.intellij.execution.ExecutionException;
import com.intellij.execution.RunContentExecutor;
import com.intellij.execution.configurations.GeneralCommandLine;
import com.intellij.execution.process.KillableColoredProcessHandler;
import com.intellij.execution.process.ProcessTerminatedListener;
import com.intellij.execution.util.ExecutionErrorDialog;
import com.intellij.openapi.application.ApplicationManager;
import com.intellij.openapi.vfs.VirtualFile;
import com.intellij.platform.lsp.api.LspClient;
import com.intellij.platform.lsp.api.customization.LspCommandsSupport;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.lsp4j.Command;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Runs the tests of a project for the code lenses of the language server above test tables: {@code
 * sqrl test} with the package files of the project, in the Docker image of the CLI, in the Run tool
 * window.
 */
final class SqrlTestRunner extends LspCommandsSupport {

  /** The command of the code lenses, with the URIs of the package files of the project */
  static final String RUN_TESTS_COMMAND = "sqrl.runTests";

  private static final String TITLE = "DataSQRL Tests";

  @Override
  public void executeCommand(
      @NotNull LspClient client, @NotNull VirtualFile file, @NotNull Command command) {
    if (!RUN_TESTS_COMMAND.equals(command.getCommand())) {
      super.executeCommand(client, file, command);
      return;
    }
    var project = client.getProject();
    try {
      var commandLine =
          commandLine(
              SqrlSettings.getInstance().getState(),
              SqrlLspClientDescriptor.docker(),
              packageFiles(command),
              project.getBasePath());
      var process = new KillableColoredProcessHandler(commandLine);
      ProcessTerminatedListener.attach(process);
      ApplicationManager.getApplication()
          .invokeLater(
              () ->
                  new RunContentExecutor(project, process)
                      .withTitle(TITLE)
                      .withActivateToolWindow(true)
                      .run());
    } catch (ExecutionException e) {
      ApplicationManager.getApplication()
          .invokeLater(() -> ExecutionErrorDialog.show(e, TITLE, project));
    }
  }

  /** The package files of a command, which the server passes as URIs. */
  static List<Path> packageFiles(Command command) {
    var files = new ArrayList<Path>();
    for (var argument : command.getArguments() == null ? List.of() : command.getArguments()) {
      var uri = argument instanceof JsonPrimitive json ? json.getAsString() : argument.toString();
      files.add(Path.of(URI.create(uri)));
    }
    return files;
  }

  /**
   * The command that runs the tests in the Docker image of the CLI, which has the services that
   * tests need, e.g. Kafka and Postgres, with the directory of the IDE project, which contains the
   * packages that the project includes. With the jar of the CLI, that jar runs in the image.
   *
   * @param settings how the language server is started
   * @param docker the Docker executable
   * @param packageFiles the package files of the project
   * @param projectBase the directory of the IDE project, if any
   */
  static GeneralCommandLine commandLine(
      SqrlSettings.Options settings,
      String docker,
      List<Path> packageFiles,
      @Nullable String projectBase)
      throws ExecutionException {
    if (packageFiles.isEmpty()) {
      throw new ExecutionException("The script is not part of a project with a package file.");
    }
    var base = projectBase == null ? null : Path.of(projectBase);
    var root =
        base != null && packageFiles.stream().allMatch(file -> file.startsWith(base))
            ? base
            : packageFiles.get(0).getParent();
    var command =
        ServerCommand.dockerTest(
            docker,
            settings.dockerImage.strip(),
            root.toString(),
            settings.launcher == Launcher.JAR ? SqrlLspClientDescriptor.jar(settings) : null,
            relative(root, packageFiles));
    return new GeneralCommandLine(command)
        .withWorkDirectory(root.toFile())
        .withParentEnvironmentType(GeneralCommandLine.ParentEnvironmentType.CONSOLE)
        .withCharset(StandardCharsets.UTF_8);
  }

  /** Paths relative to a directory, with `/` also on Windows, e.g. for the Linux image. */
  private static List<String> relative(Path dir, List<Path> files) {
    return files.stream().map(file -> dir.relativize(file).toString().replace('\\', '/')).toList();
  }
}
