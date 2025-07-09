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
package com.datasqrl.config;

import static org.assertj.core.api.Assertions.assertThat;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class ConnectorFactoryFactoryImplTest {

  private SqrlConfig config;
  private ErrorCollector errors;

  @BeforeEach
  void setUp() {
    config = SqrlConfig.createCurrentVersion();
    errors = ErrorCollector.root();
  }

  @Test
  void givenConfigWithConnectors_whenGetConnectors_thenReturnsConnectorsConfig() {
    var connectorsSubConfig = config.getSubConfig("connectors");
    connectorsSubConfig
        .getSubConfig("jdbc")
        .setProperty("url", "jdbc:postgresql://localhost:5432/db");

    var connectorFactory = new ConnectorFactoryFactoryImpl(new PackageJsonImpl(config));

    var connectorsConfig = connectorFactory.getConfig("jdbc");

    assertThat(connectorsConfig).isNotNull();
  }

  @Test
  void
      givenTestFlinkConfig_whenReadFlinkConnectorsPrint_thenReturnsInheritedConnectorConfiguration() {
    var testConfigPath = Path.of("src/test/resources/config/test-flink-config.json");
    var packageJson = ConfigLoaderUtils.loadUnresolvedConfig(errors, List.of(testConfigPath));

    var connectorFactory = new ConnectorFactoryFactoryImpl(packageJson);
    var printConnectorConf = connectorFactory.getConfig("print").toMap();

    assertThat(printConnectorConf.get("connector")).contains("print");
    assertThat(printConnectorConf.get("print-identifier")).contains("${sqrl:table-name}");
  }
}
