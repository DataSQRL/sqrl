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

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.cli.AssertStatusHook;
import com.datasqrl.cli.DatasqrlCli;
import com.datasqrl.cli.InitCmd.ProjectType;
import java.nio.file.Path;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * Ensures that every project scaffolded by {@code init} compiles out of the box, for both the prod
 * and the test package configuration.
 */
class InitProjectCompileTest {

  private static final String PROJECT_NAME = "myproject";

  @TempDir private Path tempDir;

  @ParameterizedTest
  @EnumSource(ProjectType.class)
  void givenInitializedProject_whenCompileProdAndTestPackages_thenSucceeds(ProjectType type) {
    execute(tempDir, "init", type.name().toLowerCase(), PROJECT_NAME);

    var projectDir = tempDir.resolve(PROJECT_NAME);
    execute(projectDir, "compile", PROJECT_NAME + "-prod-package.json");
    execute(projectDir, "compile", PROJECT_NAME + "-test-package.json");
  }

  private static void execute(Path workspaceDir, String... args) {
    var statusHook = new AssertStatusHook();
    var code = new DatasqrlCli(workspaceDir, statusHook, true).getCmd().execute(args);

    assertThat(statusHook.isSuccess())
        .as("'%s' failed: %s", String.join(" ", args), statusHook.getMessages())
        .isTrue();
    assertThat(code).isZero();
  }
}
