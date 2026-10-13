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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.gson.JsonPrimitive;
import com.intellij.execution.ExecutionException;
import com.intellij.openapi.application.PathManager;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Properties;
import org.eclipse.lsp4j.Command;
import org.junit.Test;

public class ServerCommandTest {

  @Test
  public void givenDockerImage_whenDocker_thenMountsTheProjectAtItsPathOnly() {
    assertEquals(
        List.of(
            "/usr/local/bin/docker",
            "run",
            "--interactive",
            "--rm",
            "--tmpfs",
            "/workspace",
            "--volume",
            "/home/me/my project:/home/me/my project",
            "datasqrl/cmd:latest",
            "lsp"),
        ServerCommand.docker(
            "/usr/local/bin/docker", "datasqrl/cmd:latest", "/home/me/my project"));
  }

  @Test
  public void givenOperatingSystem_whenDockerExecutable_thenItHasTheNameOnThatSystem() {
    assertEquals("docker.exe", SqrlLspClientDescriptor.dockerExecutable(true));
    assertEquals("docker", SqrlLspClientDescriptor.dockerExecutable(false));
  }

  @Test
  public void givenWindows_whenServerInDocker_thenAsksForTheJarInstead() throws Exception {
    var error =
        assertThrows(
            ExecutionException.class, () -> SqrlLspClientDescriptor.checkServerInDocker(true));

    assertTrue(error.getMessage(), error.getMessage().contains("jar of the CLI"));
    SqrlLspClientDescriptor.checkServerInDocker(false);
  }

  @Test
  public void givenBuildForRelease_whenDefaultImage_thenItIsTheImageOfThatRelease() {
    var release = new Properties();
    release.setProperty("image", "datasqrl/cmd:1.2.3");

    assertEquals("datasqrl/cmd:1.2.3", SqrlSettings.defaultDockerImage(release));
    // A development build has no release
    assertEquals("datasqrl/cmd:latest", SqrlSettings.defaultDockerImage(new Properties()));
    assertEquals(SqrlSettings.defaultDockerImage(), new SqrlSettings.Options().dockerImage);
  }

  @Test
  public void givenServer_whenWorkDirectory_thenItIsADirectoryOfTheIde() throws Exception {
    var dir = SqrlLspClientDescriptor.workDirectory();

    assertTrue(Files.isDirectory(dir));
    assertEquals(Path.of(PathManager.getSystemPath(), "sqrl-lsp"), dir);
  }

  @Test
  public void givenJar_whenJar_thenRunsItWithTheOptionsOfTheCli() {
    var command = ServerCommand.jar("/jdk/bin/java", "/cli/sqrl-cli.jar");

    assertEquals("/jdk/bin/java", command.get(0));
    assertEquals(ServerCommand.JVM_OPTIONS, command.subList(1, command.size() - 3));
    assertEquals(
        List.of("-jar", "/cli/sqrl-cli.jar", "lsp"),
        command.subList(command.size() - 3, command.size()));
  }

  @Test
  public void givenProject_whenDockerTest_thenRunsTheTestsInTheImageWithTheJar() {
    assertEquals(
        List.of(
            "docker",
            "run",
            "--rm",
            "--volume",
            "/repo:/workspace",
            "--volume",
            "/cli/sqrl-cli.jar:/opt/sqrl/sqrl-cli.jar",
            "datasqrl/cmd:latest",
            "test",
            "dedup/data_compaction_package_local.json"),
        ServerCommand.dockerTest(
            "docker",
            "datasqrl/cmd:latest",
            "/repo",
            "/cli/sqrl-cli.jar",
            List.of("dedup/data_compaction_package_local.json")));
  }

  @Test
  public void givenCodeLensCommand_whenCommandLine_thenRunsTheTestsOfThePackageFiles()
      throws Exception {
    var dir = Files.createTempDirectory("sqrl-test-runner");
    var cli = Files.createFile(dir.resolve("sqrl-cli.jar"));
    var settings = new SqrlSettings.Options();
    settings.launcher = SqrlSettings.Launcher.JAR;
    settings.jarPath = cli.toString();
    var packageFile = dir.resolve("project/package.json");
    var command =
        new Command(
            "▶ Run tests",
            SqrlTestRunner.RUN_TESTS_COMMAND,
            List.of(new JsonPrimitive(packageFile.toUri().toString())));

    var commandLine =
        SqrlTestRunner.commandLine(
            settings, "docker", SqrlTestRunner.packageFiles(command), dir.toString());

    assertEquals(dir.toFile(), commandLine.getWorkDirectory());
    var parameters = commandLine.getParametersList().getList();
    assertTrue(parameters.contains(cli + ":/opt/sqrl/sqrl-cli.jar"));
    assertEquals(
        List.of("test", "project/package.json"),
        parameters.subList(parameters.size() - 2, parameters.size()));
  }
}
