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
package com.datasqrl.engine.server;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.config.PackageJson;
import com.datasqrl.engine.IExecutionEngine;
import com.datasqrl.engine.database.relational.DuckDBEngineFactory;
import com.datasqrl.engine.database.relational.SnowflakeEngineFactory;
import com.google.auto.service.AutoService;
import com.google.inject.Inject;
import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@AutoService(EngineFactory.class)
public class VertxEngineFactory extends GenericJavaServerEngineFactory {

  public static final String ENGINE_NAME = "vertx";

  @Override
  public String getEngineName() {
    return ENGINE_NAME;
  }

  @Override
  public Class<? extends IExecutionEngine> getFactoryClass() {
    return VertxEngine.class;
  }

  public static class VertxEngine extends GenericJavaServerEngine {

    @Inject
    public VertxEngine(PackageJson packageJson) {
      super(ENGINE_NAME, packageJson, getQueryEngineConfigs(packageJson));
    }

    // TODO ferenc: This is temporary until we get away from JsonObject,
    //  after that this should be generalized
    private static List<JsonObject> getQueryEngineConfigs(PackageJson packageJson) {
      var configs = new ArrayList<JsonObject>();

      getDuckDbConfig(packageJson).map(configs::add);
      getSnowflakeConfig(packageJson).map(configs::add);

      return List.copyOf(configs);
    }

    private static Optional<JsonObject> getDuckDbConfig(PackageJson packageJson) {
      var url = getUrl(packageJson, DuckDBEngineFactory.ENGINE_NAME);
      if (url == null) {
        return Optional.empty();
      }

      return Optional.of(JsonObject.of("duckDbConfig", JsonObject.of("url", url)));
    }

    private static Optional<JsonObject> getSnowflakeConfig(PackageJson packageJson) {
      var url = getUrl(packageJson, SnowflakeEngineFactory.ENGINE_NAME);
      if (url == null) {
        return Optional.empty();
      }

      return Optional.of(JsonObject.of("snowflakeConfig", JsonObject.of("url", url)));
    }

    private static String getUrl(PackageJson packageJson, String engineName) {
      if (!packageJson.getEnabledEngines().contains(engineName)) {
        return null;
      }

      return packageJson
          .getEngines()
          .getEngineConfigOrEmpty(engineName)
          .getSetting("url", Optional.empty());
    }
  }
}
