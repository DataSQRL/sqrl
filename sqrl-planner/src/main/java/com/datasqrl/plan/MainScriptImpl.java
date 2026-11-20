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
package com.datasqrl.plan;

import com.datasqrl.config.PackageJson;
import com.datasqrl.config.WorkspacePaths;
import com.datasqrl.deployment.model.MutationDatabaseModel;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ConfigLoaderUtils;
import com.datasqrl.util.FileUtil;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor
public class MainScriptImpl implements MainScript {

  private final PackageJson config;
  private final WorkspacePaths workspacePaths;
  private final ErrorCollector errors;

  @Override
  public String getContent() {
    var mainScript =
        getPath()
            .orElseThrow(
                () ->
                    errors.exception(
                        "Could not find main SQRL script file: %s",
                        config.getScriptConfig().getMainScript()));

    return FileUtil.readFile(mainScript);
  }

  @Override
  public Optional<Path> getPath() {
    var mainScriptPath =
        workspacePaths.resolveConfiguredPath(
            "script.main", config.getScriptConfig().getMainScript(), errors);

    return Files.exists(mainScriptPath) ? Optional.of(mainScriptPath) : Optional.empty();
  }

  @Override
  public Optional<MutationDatabaseModel> getMutationDatabase() {
    var database = config.getScriptConfig().getDatabase();
    if (database.isEmpty()) {
      return Optional.empty();
    }
    var path = workspacePaths.resolveConfiguredPath("script.database", database.get(), errors);
    if (!Files.exists(path)) {
      throw errors.exception("Could not find database definition file: %s", database.get());
    }

    return Optional.of(ConfigLoaderUtils.loadMutationDatabase(path, errors));
  }
}
