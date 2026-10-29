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
package com.datasqrl.eclipse;

import com.google.gson.JsonPrimitive;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.IPath;
import org.eclipse.debug.core.DebugPlugin;
import org.eclipse.debug.core.ILaunchManager;
import org.eclipse.debug.core.Launch;
import org.eclipse.lsp4e.command.LSPCommandHandler;
import org.eclipse.lsp4j.Command;

/**
 * Runs the tests of a project for the code lenses above test tables: {@code sqrl test} with the
 * package files of the project, in the Docker image of the CLI, with its output in the Console.
 */
public final class RunTestsHandler extends LSPCommandHandler {

  private static final String TITLE = "DataSQRL Tests";

  @Override
  public Object execute(ExecutionEvent event, Command command, IPath path)
      throws ExecutionException {
    var packageFiles = packageFiles(command);
    if (packageFiles.isEmpty()) {
      throw new ExecutionException("The script is not part of a project with a package file.");
    }
    var root = root(packageFiles);
    var preferences = SqrlPreferences.store();
    try {
      var jar =
          SqrlPreferences.LAUNCHER_JAR.equals(preferences.getString(SqrlPreferences.LAUNCHER))
              ? SqrlConnectionProvider.jar()
              : null;
      var process =
          new ProcessBuilder(
                  ServerCommand.dockerTest(
                      preferences.getString(SqrlPreferences.DOCKER).strip(),
                      preferences.getString(SqrlPreferences.DOCKER_IMAGE).strip(),
                      root,
                      jar,
                      packageFiles))
              .directory(root.toFile())
              .start();
      var launch = new Launch(null, ILaunchManager.RUN_MODE, null);
      DebugPlugin.getDefault().getLaunchManager().addLaunch(launch);
      DebugPlugin.newProcess(launch, process, TITLE);
    } catch (IOException e) {
      throw new ExecutionException("Could not run the tests: " + e.getMessage(), e);
    }
    return null;
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
   * The project of the package files in the workspace, which contains the packages that the project
   * includes, else the directory of the first package file.
   */
  static Path root(List<Path> packageFiles) {
    for (var project : ResourcesPlugin.getWorkspace().getRoot().getProjects()) {
      var location = project.getLocation();
      if (project.isOpen() && location != null) {
        var dir = location.toPath();
        if (packageFiles.stream().allMatch(file -> file.startsWith(dir))) {
          return dir;
        }
      }
    }
    return packageFiles.get(0).getParent();
  }
}
