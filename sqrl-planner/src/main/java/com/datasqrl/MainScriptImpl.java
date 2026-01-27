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
package com.datasqrl;

import com.datasqrl.config.PackageJson;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.plan.MainScript;
import com.datasqrl.util.FileUtil;
import jakarta.inject.Inject;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_ = @Inject)
public class MainScriptImpl implements MainScript {

  private final PackageJson config;
  private final ResourceResolver resourceResolver;

  @Override
  public String getContent() {
    var mainScript =
        config
            .getScriptConfig()
            .getMainScript()
            .map(Path::of)
            .flatMap(resourceResolver::resolveFile)
            .orElseThrow(() -> new RuntimeException("Could not find main sqrl script file"));
    return FileUtil.readFile(mainScript);
  }

  @Override
  public Optional<Path> getPath() {
    return config
        .getScriptConfig()
        .getMainScript()
        .map(Path::of)
        .flatMap(resourceResolver::resolveFile);
  }
}
