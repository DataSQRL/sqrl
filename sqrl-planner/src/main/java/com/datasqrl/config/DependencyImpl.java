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
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
@EqualsAndHashCode
public class DependencyImpl implements Dependency {

  @Constraints.Default @Setter private String folder = null;

  // For now, keep "name" as a backup to not break existing configs.
  @Deprecated @Constraints.Default private String name = null;

  public DependencyImpl(String folder) {
    this.folder = folder;
  }

  public DependencyImpl(SqrlConfig sqrlConfig) {
    folder = sqrlConfig.asString("folder").getOptional().orElse(null);
    name = sqrlConfig.asString("name").getOptional().orElse(null);
  }

  @Override
  public String getFolder(@Nullable ErrorCollector errors) {
    if (folder == null) {
      if (errors != null) {
        errors.warn(
            "The \"name\" key is deprecated in \"dependencies\" and it will be removed in an upcoming release. Use the \"folder\" key instead.");
      }

      return name;
    }

    return folder;
  }

  @Override
  public String toString() {
    return this.getFolder(null);
  }

  /**
   * Normalizes a dependency and uses the dependency package name as the folder unless it is
   * explicitly specified.
   *
   * @param defaultFolder dependency package name
   * @return the normalized {@link Dependency}
   */
  @Override
  public Dependency normalize(String defaultFolder, ErrorCollector errors) {
    errors.checkFatal(
        !Strings.isNullOrEmpty(defaultFolder), "Invalid dependency folder: %s", defaultFolder);
    String folder;
    if (Strings.isNullOrEmpty(this.getFolder(null))) {
      folder = defaultFolder;
    } else {
      folder = this.getFolder(errors);
    }
    return new DependencyImpl(folder);
  }
}
