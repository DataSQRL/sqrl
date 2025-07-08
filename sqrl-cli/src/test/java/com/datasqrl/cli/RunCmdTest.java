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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Path;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RunCmdTest {

  @Mock private ErrorCollector errors;
  @Mock private PackageJson.CompilerConfig sqrlCompilerConfig;
  @Mock private Configuration flinkConfig;

  private RunCmd runCmd;

  @TempDir private Path tempDir;

  @BeforeEach
  void setup() {
    runCmd = spy(new RunCmd());
  }

  @Test
  void execute_whenInternalTestExecIsTrue_shouldSkipRunPart() throws Exception {
    doNothing().when(runCmd).execute(any(), any(), any());
    runCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, true);

    runCmd.execute(errors);

    // Should call super.execute but skip DatasqrlRun
    verify(runCmd).execute(errors);
    verify(runCmd, never()).getTargetDir();
  }

  @Test
  void execute_shouldLoadConfigsAndRunDatasqrlRun() throws Exception {
    doNothing().when(runCmd).execute(any(), any(), any());
    runCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, false);

    var mockSqrlConfig = mock(PackageJson.class);
    when(mockSqrlConfig.getCompilerConfig()).thenReturn(sqrlCompilerConfig);

    Path buildDir = runCmd.getBuildDir();
    Path planDir = runCmd.getTargetDir().resolve(SqrlConstants.PLAN_DIR);

    // Mock static methods and verify arguments
    try (MockedStatic<ConfigLoaderUtils> mocked = mockStatic(ConfigLoaderUtils.class)) {
      mocked
          .when(() -> ConfigLoaderUtils.loadResolvedConfig(errors, buildDir))
          .thenReturn(mockSqrlConfig);

      mocked.when(() -> ConfigLoaderUtils.loadFlinkConfig(planDir)).thenReturn(flinkConfig);

      try (MockedConstruction<DatasqrlRun> datasqrlRunMocked =
          mockConstruction(
              DatasqrlRun.class, (mock, context) -> when(mock.run(true, true)).thenReturn(null))) {
        runCmd.execute(errors);

        // Verify exact arguments
        mocked.verify(() -> ConfigLoaderUtils.loadResolvedConfig(errors, buildDir));
        mocked.verify(() -> ConfigLoaderUtils.loadFlinkConfig(planDir));

        DatasqrlRun constructed = datasqrlRunMocked.constructed().get(0);
        verify(constructed).run(true, true);
      }
    }
  }

  @Test
  void getGoal_shouldReturnRun() {
    assertThat(runCmd.getGoal()).isEqualTo(ExecutionGoal.RUN);
  }
}
