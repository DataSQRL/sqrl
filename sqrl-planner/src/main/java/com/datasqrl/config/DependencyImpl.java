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

import com.datasqrl.error.ErrorCollector;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@EqualsAndHashCode
@Setter
@AllArgsConstructor
public class DependencyImpl implements Dependency {
  //
  @Constraints.Default String name = null;

  public DependencyImpl() {}

  public DependencyImpl(SqrlConfig sqrlConfig) {
    name = sqrlConfig.asString("name").getOptional().orElse(null);
  }

  @Override
  public String toString() {
    return getName();
  }

  /**
   * Normalizes a dependency and uses the dependency package name as the name unless it is
   * explicitly specified.
   *
   * @param defaultName
   * @return
   */
  @Override
  public Dependency normalize(String defaultName, ErrorCollector errors) {
    errors.checkFatal(
        !Strings.isNullOrEmpty(defaultName), "Invalid dependency name: %s", defaultName);
    String name;
    if (Strings.isNullOrEmpty(this.getName())) {
      name = defaultName;
    } else {
      name = this.getName();
    }
    return new DependencyImpl(name);
  }
}
