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

import com.datasqrl.graphql.APISource;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.module.resolver.ResourceResolver;
import com.google.inject.Inject;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import lombok.Value;

@Value
public class GraphqlSourceFactory {
  Optional<APISource> apiSchema;
  List<APISource> operations;

  @Inject
  public GraphqlSourceFactory(ScriptFiles scriptFiles, ResourceResolver resolver) {
    apiSchema = scriptFiles.getConfig().getGraphql().map(file -> resolvePath(file, resolver));
    operations =
        scriptFiles.getConfig().getOperations().stream()
            .map(file -> resolvePath(file, resolver))
            .toList();
  }

  private static APISource resolvePath(String file, ResourceResolver resolver) {
    try {
      Path relativePath = Path.of(file);
      Optional<Path> absolutePath = resolver.resolveFile(relativePath);
      return APISource.of(
          relativePath,
          Files.readString(
              absolutePath.orElseThrow(
                  () -> new IllegalArgumentException("Could not read file: " + relativePath))));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
