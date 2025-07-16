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
import static org.mockito.Mockito.*;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.error.ErrorCollector;
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
class ExecuteCmdTest {

  @Mock private ErrorCollector errors;
  @Mock private Configuration flinkConfig;

  private ExecuteCmd executeCmd;

  @TempDir private Path tempDir;

  @BeforeEach
  void setup() {
    executeCmd = spy(new ExecuteCmd());
  }

  @Test
  void runInternal_shouldStartServicesAndRunDatasqrlRun() throws Exception {
    executeCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, false);

    Path buildDir = executeCmd.getBuildDir();
    Path planDir = executeCmd.getTargetDir().resolve(SqrlConstants.PLAN_DIR);

    // Mock static methods and verify arguments
    try (MockedStatic<ConfigLoaderUtils> mocked = mockStatic(ConfigLoaderUtils.class)) {

      var mockSqrlConfig = mock(PackageJson.class);
      mocked
          .when(() -> ConfigLoaderUtils.loadResolvedConfig(errors, buildDir))
          .thenReturn(mockSqrlConfig);

      mocked.when(() -> ConfigLoaderUtils.loadFlinkConfig(planDir)).thenReturn(flinkConfig);

      try (MockedConstruction<DependentServiceManager> serviceManagerMocked =
              mockConstruction(DependentServiceManager.class);
          MockedConstruction<DatasqrlRun> datasqrlRunMocked =
              mockConstruction(
                  DatasqrlRun.class,
                  (mock, context) -> when(mock.run(true, true)).thenReturn(null))) {

        executeCmd.runInternal(errors);

        // Verify service manager was created and started
        assertThat(serviceManagerMocked.constructed()).hasSize(1);
        DependentServiceManager serviceManager = serviceManagerMocked.constructed().get(0);
        verify(serviceManager).startServices();

        // Verify exact arguments
        mocked.verify(() -> ConfigLoaderUtils.loadResolvedConfig(errors, buildDir));
        mocked.verify(() -> ConfigLoaderUtils.loadFlinkConfig(planDir));

        DatasqrlRun constructed = datasqrlRunMocked.constructed().get(0);
        verify(constructed).run(true, true);
      }
    }
  }
}
