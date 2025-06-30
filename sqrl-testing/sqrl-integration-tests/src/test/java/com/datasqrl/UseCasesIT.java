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
package com.datasqrl;

import static org.assertj.core.api.Assertions.fail;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.cli.DatasqrlCli;
import com.datasqrl.cli.StatusHook;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.flink.calcite.shaded.com.google.common.base.Strings;

/** Tests some use cases in the test/resources/usecases folder using the `test` command. */
public class UseCasesIT {
  private static final Path RESOURCES = Path.of("src/test/resources/usecases");

  public void execute(String path, String script, String graphql) {
    execute("test", path, script, graphql, null);
  }

  public void execute(
      String goal, String path, String script, String graphql, String testSuffix, String... args) {
    var rootDir = RESOURCES.resolve(path);
    List<String> argsList = new ArrayList<>();
    argsList.add(goal);
    argsList.add(script);
    if (!Strings.isNullOrEmpty(graphql)) {
      argsList.add(graphql);
    }
    if (testSuffix != null) {
      argsList.add("-s");
      argsList.add("snapshots-" + testSuffix);
      argsList.add("--tests");
      argsList.add("tests-" + testSuffix);
    }
    argsList.addAll(Arrays.asList(args));

    execute(rootDir, new AssertStatusHook(), argsList.toArray(String[]::new));
  }

  protected void compile(String path, String script, String graphql) {
    var rootDir = RESOURCES.resolve(path);
    List<String> argsList = new ArrayList<>();
    argsList.add("compile");
    argsList.add(script);
    if (!Strings.isNullOrEmpty(graphql)) {
      argsList.add(graphql);
    }
    execute(RESOURCES.resolve(path), new AssertStatusHook(), argsList.toArray(a -> new String[a]));
  }

  public static int execute(Path rootDir, StatusHook hook, String... args) {
    var cli = new DatasqrlCli(rootDir, hook, true);
    var exitCode = cli.getCmd().execute(args) + (hook.isSuccess() ? 0 : 1);
    if (exitCode != 0) {
      fail("");
    }
    return exitCode;
  }

  protected static Path getProjectRoot(Path script) {
    var currentPath = script.toAbsolutePath();
    while (!currentPath.getFileName().toString().equals("sqrl-testing")) {
      currentPath = currentPath.getParent();
    }

    return currentPath.getParent();
  }

  protected static Path getProjectRoot() {
    var userDir = System.getProperty("user.dir");
    var path = Path.of(userDir);
    return getProjectRoot(path);
  }
}
