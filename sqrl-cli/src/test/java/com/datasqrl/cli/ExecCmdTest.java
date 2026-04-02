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

import static org.mockito.Mockito.*;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.SqrlConstants;
import com.datasqrl.env.GlobalEnvironmentStore;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.OsProcessManager;
import java.nio.file.Path;
import java.util.Map;
import org.apache.flink.configuration.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ExecCmdTest {

  @Mock private ErrorCollector errors;
  @Mock private Configuration flinkConfig;
  @Mock private OsProcessManager osProcessManager;
  @Mock private DatasqrlRun datasqrlRun;

  private ExecCmd execCmd;

  @TempDir private Path tempDir;

  @BeforeEach
  void setup() {
    execCmd = spy(new ExecCmd());
    doReturn(osProcessManager).when(execCmd).getOsProcessManager();
  }

  @Test
  void runInternal_shouldStartServicesAndRunDatasqrlRun() throws Exception {
    execCmd.cli = new DatasqrlCli(tempDir, StatusHook.NONE, false);

    Path buildDir = execCmd.getBuildDir();
    Path planDir = execCmd.getTargetFolder().resolve(SqrlConstants.PLAN_DIR);

    var mockSqrlConfig = mock(PackageJson.class);
    var mockEnv = Map.of("key", "value");

    try (MockedStatic<ConfigLoaderUtils> configMocked = mockStatic(ConfigLoaderUtils.class);
        MockedStatic<GlobalEnvironmentStore> envMocked = mockStatic(GlobalEnvironmentStore.class);
        MockedStatic<DatasqrlRun> runMocked = mockStatic(DatasqrlRun.class)) {

      configMocked
          .when(() -> ConfigLoaderUtils.loadResolvedConfig(errors, buildDir))
          .thenReturn(mockSqrlConfig);
      configMocked.when(() -> ConfigLoaderUtils.loadFlinkConfig(planDir)).thenReturn(flinkConfig);
      envMocked.when(GlobalEnvironmentStore::getAll).thenReturn(mockEnv);
      runMocked
          .when(() -> DatasqrlRun.blocking(eq(planDir), eq(mockSqrlConfig), any(), eq(mockEnv)))
          .thenReturn(datasqrlRun);
      when(datasqrlRun.run()).thenReturn(null);

      execCmd.runInternal(errors);

      verify(osProcessManager).startDependentServices(planDir);
      configMocked.verify(() -> ConfigLoaderUtils.loadResolvedConfig(errors, buildDir));
      configMocked.verify(() -> ConfigLoaderUtils.loadFlinkConfig(planDir));
      envMocked.verify(GlobalEnvironmentStore::getAll);
      runMocked.verify(
          () -> DatasqrlRun.blocking(eq(planDir), eq(mockSqrlConfig), any(), eq(mockEnv)));
      verify(datasqrlRun).run();
    }
  }
}
