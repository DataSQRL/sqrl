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

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * The commands of the CLI: the one that starts the SQRL language server, {@code sqrl lsp}, and the
 * one that runs the tests of a project, {@code sqrl test}.
 */
final class ServerCommand {

  /** The JVM options that the CLI needs, as in {@code SQRL_JVM_TOOL_OPTS} of its Docker image */
  static final List<String> JVM_OPTIONS =
      List.of(
          "--add-exports=java.base/sun.net.util=ALL-UNNAMED",
          "--add-exports=java.rmi/sun.rmi.registry=ALL-UNNAMED",
          "--add-exports=jdk.compiler/com.sun.tools.javac.api=ALL-UNNAMED",
          "--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
          "--add-exports=jdk.compiler/com.sun.tools.javac.parser=ALL-UNNAMED",
          "--add-exports=jdk.compiler/com.sun.tools.javac.tree=ALL-UNNAMED",
          "--add-exports=jdk.compiler/com.sun.tools.javac.util=ALL-UNNAMED",
          "--add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
          "--add-opens=java.base/java.lang=ALL-UNNAMED",
          "--add-opens=java.base/java.net=ALL-UNNAMED",
          "--add-opens=java.base/java.io=ALL-UNNAMED",
          "--add-opens=java.base/java.nio=ALL-UNNAMED",
          "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
          "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
          "--add-opens=java.base/java.text=ALL-UNNAMED",
          "--add-opens=java.base/java.time=ALL-UNNAMED",
          "--add-opens=java.base/java.util=ALL-UNNAMED",
          "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED",
          "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED",
          "--add-opens=java.base/java.util.concurrent.locks=ALL-UNNAMED");

  /** Where the Docker image of the CLI starts, which it requires to exist */
  static final String WORKSPACE = "/workspace";

  /** Where the Docker image of the CLI has the jar of the CLI */
  static final String IMAGE_JAR = "/opt/sqrl/sqrl-cli.jar";

  private ServerCommand() {}

  /**
   * Runs the server in the Docker image of the CLI. The editor refers to files by their paths, so
   * the projects are mounted at their own paths. The image starts in {@link #WORKSPACE}, which it
   * requires, so that is an empty temporary directory: files that checks create at relative paths,
   * e.g. a local Iceberg warehouse, do not end up in a project.
   */
  static List<String> docker(String docker, String image, Collection<Path> projectDirs) {
    var command =
        new ArrayList<>(List.of(docker, "run", "--interactive", "--rm", "--tmpfs", WORKSPACE));
    for (var dir : mounts(projectDirs)) {
      command.addAll(List.of("--volume", dir + ":" + dir));
    }
    command.addAll(List.of(image, "lsp"));
    return command;
  }

  /** The directories without the ones inside others, which are mounted with them. */
  static List<Path> mounts(Collection<Path> dirs) {
    var normalized = dirs.stream().map(dir -> dir.toAbsolutePath().normalize()).distinct().toList();
    return normalized.stream()
        .filter(dir -> normalized.stream().noneMatch(other -> !other.equals(dir) && dir.startsWith(other)))
        .toList();
  }

  static List<String> jar(String java, String jar) {
    var command = new ArrayList<String>();
    command.add(java);
    command.addAll(JVM_OPTIONS);
    command.addAll(List.of("-jar", jar, "lsp"));
    return command;
  }

  /**
   * Runs the tests of a project in the Docker image of the CLI, which has the services that tests
   * need, e.g. Kafka and Postgres. The root is at {@link #WORKSPACE}, so that the packages that the
   * project includes are there too. With a jar, that jar runs instead of the one of the image.
   *
   * @param root the directory that contains the package files
   * @param packageFiles the package files of the project
   */
  static List<String> dockerTest(
      String docker, String image, Path root, String jar, List<Path> packageFiles) {
    var command =
        new ArrayList<>(List.of(docker, "run", "--rm", "--volume", root + ":" + WORKSPACE));
    if (jar != null) {
      command.addAll(List.of("--volume", jar + ":" + IMAGE_JAR));
    }
    command.addAll(List.of(image, "test"));
    packageFiles.forEach(
        file -> command.add(root.relativize(file).toString().replace('\\', '/')));
    return command;
  }
}
