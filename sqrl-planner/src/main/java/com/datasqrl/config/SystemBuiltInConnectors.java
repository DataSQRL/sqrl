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

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public enum SystemBuiltInConnectors {
  PRINT_SINK(Name.system("print"), ExternalDataType.sink),
  LOGGER(Name.system("logger"), ExternalDataType.sink),
  LOG_ENGINE(Name.system("log"), ExternalDataType.sink);

  final Name name;
  final ExternalDataType type;

  @Override
  public String toString() {
    return name.getCanonical();
  }

  public static Optional<SystemBuiltInConnectors> forExport(Name name) {
    for (SystemBuiltInConnectors connector : values()) {
      if (connector.type.isSink() && connector.name.equals(name)) {
        return Optional.of(connector);
      }
    }
    return Optional.empty();
  }

  public static Optional<SystemBuiltInConnectors> forExport(NamePath path) {
    if (path.size() == 1) {
      return forExport(path.getFirst());
    }
    return Optional.empty();
  }
}
