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

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import org.eclipse.core.resources.IProject;
import org.eclipse.core.resources.ResourcesPlugin;
import org.eclipse.core.runtime.IPath;
import org.eclipse.core.runtime.Platform;
import org.eclipse.lsp4e.server.ProcessStreamConnectionProvider;

/**
 * Starts the SQRL language server, one for the workspace, in the Docker image of the CLI or from its
 * jar.
 */
public final class SqrlConnectionProvider extends ProcessStreamConnectionProvider {

  @Override
  public void start() throws IOException {
    var preferences = SqrlPreferences.store();
    if (SqrlPreferences.LAUNCHER_JAR.equals(preferences.getString(SqrlPreferences.LAUNCHER))) {
      setCommands(ServerCommand.jar(java(preferences.getString(SqrlPreferences.JAVA)), jar()));
    } else {
      if (Platform.OS_WIN32.equals(Platform.getOS())) {
        throw new IOException(
            "On Windows, the language server cannot run in Docker yet. Start it from the jar of the"
                + " CLI in Window | Preferences | DataSQRL.");
      }
      setCommands(
          ServerCommand.docker(
              preferences.getString(SqrlPreferences.DOCKER).strip(),
              preferences.getString(SqrlPreferences.DOCKER_IMAGE).strip(),
              projectDirs()));
    }
    setWorkingDirectory(workDirectory().toString());
    super.start();
  }

  @Override
  public Object getInitializationOptions(URI rootUri) {
    return SqrlPreferences.settings(SqrlPreferences.store());
  }

  /** The workspace and the open projects, which may be elsewhere, e.g. imported ones. */
  private static List<Path> projectDirs() {
    var root = ResourcesPlugin.getWorkspace().getRoot();
    var dirs = new ArrayList<Path>();
    dirs.add(root.getLocation().toPath());
    Arrays.stream(root.getProjects())
        .filter(IProject::isOpen)
        .map(IProject::getLocation)
        .filter(Objects::nonNull)
        .map(IPath::toPath)
        .forEach(dirs::add);
    return dirs;
  }

  /**
   * The working directory of the server, in the state of the plugin: files that checks create at
   * relative paths, e.g. a local Iceberg warehouse, end up there instead of in a project.
   */
  private static Path workDirectory() throws IOException {
    var bundle = Platform.getBundle(SqrlPreferences.BUNDLE);
    return Files.createDirectories(Platform.getStateLocation(bundle).toPath().resolve("sqrl-lsp"));
  }

  /**
   * The configured Java, else the one of {@code JAVA_HOME}, else the one on the PATH: the runtime of
   * Eclipse may be too new for the CLI, since Hadoop, which e.g. Iceberg uses, fails on Java 23 and
   * later.
   */
  static String java(String configured) {
    if (!configured.isBlank()) {
      return configured.strip();
    }
    var javaHome = System.getenv("JAVA_HOME");
    if (javaHome != null && !javaHome.isBlank()) {
      for (var name : List.of("java", "java.exe")) {
        var java = Path.of(javaHome.strip(), "bin", name);
        if (Files.isRegularFile(java)) {
          return java.toString();
        }
      }
    }
    return "java";
  }

  static String jar() throws IOException {
    var jar = SqrlPreferences.store().getString(SqrlPreferences.JAR).strip();
    if (jar.isEmpty() || !Files.isRegularFile(Path.of(jar))) {
      throw new IOException(
          "Set the path of sqrl-cli.jar in Window | Preferences | DataSQRL"
              + (jar.isEmpty() ? "." : ", " + jar + " does not exist."));
    }
    return Path.of(jar).toAbsolutePath().toString();
  }
}
