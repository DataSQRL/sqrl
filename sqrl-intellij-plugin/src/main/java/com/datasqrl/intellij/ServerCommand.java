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

import java.util.ArrayList;
import java.util.List;
import org.jetbrains.annotations.Nullable;

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

  private ServerCommand() {}

  /**
   * Runs the server in the Docker image of the CLI. The editor refers to files by their paths, so
   * the project is mounted at its own path. The image starts in {@link #WORKSPACE}, which it
   * requires, so that is an empty temporary directory: files that checks create at relative paths,
   * e.g. a local Iceberg warehouse, do not end up in the project.
   *
   * @param docker the Docker executable
   * @param image the image of the CLI, e.g. {@code datasqrl/cmd:latest}
   * @param projectDir the directory of the project
   */
  static List<String> docker(String docker, String image, String projectDir) {
    return List.of(
        docker,
        "run",
        "--interactive",
        "--rm",
        "--tmpfs",
        WORKSPACE,
        "--volume",
        projectDir + ":" + projectDir,
        image,
        "lsp");
  }

  /**
   * Runs the server from the jar of the CLI.
   *
   * @param java the Java executable
   * @param jar the jar of the CLI, {@code sqrl-cli.jar}
   */
  static List<String> jar(String java, String jar) {
    var command = new ArrayList<String>();
    command.add(java);
    command.addAll(JVM_OPTIONS);
    command.addAll(List.of("-jar", jar, "lsp"));
    return command;
  }

  /** Where the Docker image of the CLI has the jar of the CLI */
  static final String IMAGE_JAR = "/opt/sqrl/sqrl-cli.jar";

  /**
   * Runs the tests of a project in the Docker image of the CLI, which has the services that tests
   * need, e.g. Kafka and Postgres. A directory that contains the project is at {@link #WORKSPACE},
   * so that the packages that the project includes are there too.
   *
   * @param docker the Docker executable
   * @param image the image of the CLI
   * @param root the directory that is mounted, which contains the package files
   * @param jar a jar of the CLI that runs instead of the one of the image, e.g. a local build
   * @param packageFiles the package files of the project, relative to the root
   */
  static List<String> dockerTest(
      String docker, String image, String root, @Nullable String jar, List<String> packageFiles) {
    var command =
        new ArrayList<>(List.of(docker, "run", "--rm", "--volume", root + ":" + WORKSPACE));
    if (jar != null) {
      command.addAll(List.of("--volume", jar + ":" + IMAGE_JAR));
    }
    command.addAll(List.of(image, "test"));
    command.addAll(packageFiles);
    return command;
  }
}
