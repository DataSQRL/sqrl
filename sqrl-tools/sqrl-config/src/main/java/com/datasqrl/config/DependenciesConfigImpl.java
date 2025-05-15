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

import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DependenciesConfigImpl implements PackageJson.DependenciesConfig {

  public static final String DEPENDENCIES_KEY = "dependencies";
  public static final String PKG_NAME_KEY = "pkgName";

  SqrlConfig parentConfig;
  SqrlConfig sqrlConfig;

  @Override
  public void addDependency(String key, Dependency dep) {
    sqrlConfig.getSubConfig(key).setProperties(dep);
  }

  @Override
  public Optional<Dependency> getDependency(String dependency) {
    if (!sqrlConfig.hasSubConfig(dependency)) {
      // todo Optional
      return Optional.empty();
    }

    var subConfig = sqrlConfig.getSubConfig(dependency);
    return Optional.of(new DependencyImpl(subConfig));
  }

  @Override
  public Map<String, DependencyImpl> getDependencies() {
    return parentConfig.asMap(DEPENDENCIES_KEY, DependencyImpl.class).get();
  }

  //  public static LinkedHashMap<String, Dependency> fromRootConfig(@NonNull SqrlConfig config) {
  //    return config.asMap(DEPENDENCIES_KEY,Dependency.class).get();
  //  }

}
