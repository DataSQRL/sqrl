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
package com.datasqrl.engine.stream.flink;

import static org.apache.flink.table.api.config.TableConfigOptions.LOCAL_TIME_ZONE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.config.PackageJson.EnginesConfig;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FlinkStreamEngineTest {

  @Test
  void givenInternalTestExec_whenGetBaseConfiguration_thenUsesInternalTestTimeZone() {
    var engine = new FlinkStreamEngine(packageJson(Map.of()), true);

    assertThat(engine.getBaseConfiguration().get(LOCAL_TIME_ZONE))
        .isEqualTo(FlinkStreamEngine.INTERNAL_TEST_TIME_ZONE);
  }

  @Test
  void givenConfiguredTimeZone_whenInternalTestExec_thenKeepsConfiguredTimeZone() {
    var engine =
        new FlinkStreamEngine(packageJson(Map.of(LOCAL_TIME_ZONE.key(), "Asia/Kolkata")), true);

    assertThat(engine.getBaseConfiguration().get(LOCAL_TIME_ZONE)).isEqualTo("Asia/Kolkata");
  }

  @Test
  void givenNoInternalTestExec_whenGetBaseConfiguration_thenTimeZoneIsNotSet() {
    var engine = new FlinkStreamEngine(packageJson(Map.of()), false);

    assertThat(engine.getBaseConfiguration().contains(LOCAL_TIME_ZONE)).isFalse();
  }

  private static PackageJson packageJson(Map<String, Object> flinkConfig) {
    var engineConfig = mock(EngineConfig.class);
    when(engineConfig.getConfig()).thenReturn(flinkConfig);
    var engines = mock(EnginesConfig.class);
    when(engines.getEngineConfigOrEmpty(FlinkEngineFactory.ENGINE_NAME)).thenReturn(engineConfig);
    var json = mock(PackageJson.class);
    when(json.getEngines()).thenReturn(engines);
    return json;
  }
}
