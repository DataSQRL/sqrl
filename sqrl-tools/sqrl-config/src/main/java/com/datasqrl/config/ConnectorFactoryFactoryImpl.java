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

import com.google.common.base.Preconditions;
import com.google.inject.Inject;
import java.util.Optional;
import lombok.AllArgsConstructor;

/** Placeholder for future templated connector handling */
@AllArgsConstructor(onConstructor_ = @Inject)
public class ConnectorFactoryFactoryImpl implements ConnectorFactoryFactory {

  PackageJson packageJson;

  private Optional<ConnectorConf> getConnectorConfig(String connectorName) {
    var engineConfig = packageJson.getEngines().getEngineConfig("flink");
    Preconditions.checkArgument(engineConfig.isPresent(), "Missing engine configuration for Flink");
    var connectors = engineConfig.get().getConnectors();
    return connectors.getConnectorConfig(connectorName);
  }

  @Override
  public ConnectorConf getConfig(String name) {
    var connectors = packageJson.getEngines().getEngineConfigOrErr("flink").getConnectors();
    return connectors.getConnectorConfigOrErr(name);
  }

  @Override
  public Optional<ConnectorConf> getOptionalConfig(String name) {
    var connectors = packageJson.getEngines().getEngineConfigOrErr("flink").getConnectors();
    return connectors.getConnectorConfig(name);
  }
}
