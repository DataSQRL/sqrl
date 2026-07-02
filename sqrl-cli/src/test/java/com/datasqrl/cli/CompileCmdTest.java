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
package com.datasqrl.cli;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.compile.CompilationProcess;
import com.datasqrl.compile.TestPlan;
import com.datasqrl.config.ExecutionEnginesHolder;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.TestRunnerConfiguration;
import com.datasqrl.engine.PhysicalPlan;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.packager.Packager;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

@ExtendWith(MockitoExtension.class)
class CompileCmdTest {

  @Mock private PackageJson sqrlConfig;
  @Mock private TestRunnerConfiguration testConfig;
  @Mock private ExecutionEnginesHolder engineHolder;
  @Mock private Packager packager;
  @Mock private CompilationProcess compilationProcess;

  @TempDir private Path tempDir;

  @Test
  void compile_whenProjectRootIsSet_resolvesTestDirFromProjectRoot() throws Exception {
    var projectRoot = tempDir.resolve("project");
    var packageJsonPath = projectRoot.resolve("package.json");
    var testDir = projectRoot.resolve("tests");
    Files.createDirectories(testDir);
    Files.writeString(packageJsonPath, "{}");

    var compileCmd = new CompileCmd();
    compileCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, true);
    compileCmd.projectRoot = Optional.of(Path.of("project"));

    var physicalPlan = PhysicalPlan.builder().build();
    var testPlan = new TestPlan();

    when(sqrlConfig.getTestConfig()).thenReturn(testConfig);
    when(testConfig.getTestDir(projectRoot)).thenReturn(Optional.of(testDir));
    when(compilationProcess.executeCompilation(Optional.of(testDir)))
        .thenReturn(Pair.of(physicalPlan, testPlan));

    try (var configLoader = mockStatic(ConfigLoaderUtils.class);
        var springContext =
            mockConstruction(
                AnnotationConfigApplicationContext.class,
                (mock, context) -> {
                  when(mock.getBean(ExecutionEnginesHolder.class)).thenReturn(engineHolder);
                  when(mock.getBean(Packager.class)).thenReturn(packager);
                  when(mock.getBean(CompilationProcess.class)).thenReturn(compilationProcess);
                })) {
      configLoader
          .when(
              () ->
                  ConfigLoaderUtils.loadUnresolvedConfig(
                      any(ErrorCollector.class), eq(List.of(packageJsonPath))))
          .thenReturn(sqrlConfig);

      compileCmd.compile(ErrorCollector.root());

      verify(testConfig).getTestDir(projectRoot);
      verify(testConfig, never()).getTestDir(tempDir);
      verify(compilationProcess).executeCompilation(Optional.of(testDir));
    }
  }
}
