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

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.file.Path;
import java.util.List;
import org.eclipse.lsp4j.Command;
import org.junit.jupiter.api.Test;

class ServerCommandTest {

  @Test
  void givenNestedProjects_whenDocker_thenMountsOutermostAtTheirPaths() {
    var command =
        ServerCommand.docker(
            "docker",
            "datasqrl/cmd:latest",
            List.of(Path.of("/ws"), Path.of("/ws/shop"), Path.of("/elsewhere")));

    assertThat(command)
        .containsExactly(
            "docker", "run", "--interactive", "--rm", "--tmpfs", "/workspace",
            "--volume", "/ws:/ws", "--volume", "/elsewhere:/elsewhere",
            "datasqrl/cmd:latest", "lsp");
  }

  @Test
  void givenJar_whenJar_thenRunsLspWithJvmOptions() {
    var command = ServerCommand.jar("/jdk/bin/java", "/cli/sqrl-cli.jar");

    assertThat(command).startsWith("/jdk/bin/java").endsWith("-jar", "/cli/sqrl-cli.jar", "lsp");
    assertThat(command).containsAll(ServerCommand.JVM_OPTIONS);
  }

  @Test
  void givenPackageFilesAndJar_whenDockerTest_thenMountsRootAndJarWithRelativePaths() {
    var command =
        ServerCommand.dockerTest(
            "docker",
            "datasqrl/cmd:latest",
            Path.of("/ws"),
            "/cli/sqrl-cli.jar",
            List.of(Path.of("/ws/shop/package.json"), Path.of("/ws/shop/package-test.json")));

    assertThat(command)
        .containsExactly(
            "docker", "run", "--rm", "--volume", "/ws:/workspace",
            "--volume", "/cli/sqrl-cli.jar:/opt/sqrl/sqrl-cli.jar",
            "datasqrl/cmd:latest", "test", "shop/package.json", "shop/package-test.json");
  }

  @Test
  void givenCodeLensCommand_whenPackageFiles_thenConvertsUrisToPaths() {
    var command =
        new Command("Run tests", "sqrl.runTests", List.of("file:///ws/shop/package.json"));

    assertThat(RunTestsHandler.packageFiles(command))
        .containsExactly(Path.of("/ws/shop/package.json"));
  }
}
