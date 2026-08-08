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
package com.datasqrl.util;

import com.datasqrl.config.EngineFactory;
import com.datasqrl.engine.IExecutionEngine;
import com.datasqrl.engine.database.relational.AbstractJDBCQueryEngine;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class EngineUtil {

  public static Stream<EngineFactory> getAvailableQueryEngines() {
    return getChildEngineFactories(AbstractJDBCQueryEngine.class);
  }

  public static String formatEngineNames(Stream<EngineFactory> engines) {
    return engines
        .map(EngineFactory::getEngineName)
        .map(name -> '\'' + name + '\'')
        .collect(Collectors.joining(", "));
  }

  public static Stream<EngineFactory> getChildEngineFactories(
      Class<? extends IExecutionEngine> parentEngineClass) {

    return ServiceLoaderDiscovery.getAll(EngineFactory.class).stream()
        .filter(factory -> parentEngineClass.isAssignableFrom(factory.getFactoryClass()));
  }
}
