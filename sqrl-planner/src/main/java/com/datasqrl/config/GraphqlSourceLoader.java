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

import com.datasqrl.config.PackageJson.ScriptApiConfig;
import com.datasqrl.graphql.ApiSource;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import lombok.SneakyThrows;
import lombok.Value;

@Value
public class GraphqlSourceLoader {

  public static final String DEFAULT_API_VERSION = "v1";

  Map<String, ApiSources> apiByVersion;

  @Inject
  public GraphqlSourceLoader(ScriptFiles scriptFiles, ResourceResolver resolver) {
    if (!scriptFiles.getApiConfigs().isEmpty()) {

      var builder = ImmutableMap.<String, ApiSources>builder();
      for (ScriptApiConfig apiConf : scriptFiles.getApiConfigs()) {
        var sources = createApiSources(apiConf.getSchema(), apiConf.getOperations(), resolver);
        builder.put(apiConf.getVersion(), sources);
      }

      apiByVersion = builder.build();
      return;
    }

    if (scriptFiles.getGraphql().isEmpty()) {
      apiByVersion = Map.of();
      return;
    }

    var sources =
        createApiSources(scriptFiles.getGraphql().get(), scriptFiles.getOperations(), resolver);

    apiByVersion = Map.of(DEFAULT_API_VERSION, sources);
  }

  private static ApiSources createApiSources(
      String schema, List<String> operations, ResourceResolver resolver) {

    var schemaSrc = resolvePath(schema, resolver);
    var opSrc = operations.stream().map(file -> resolvePath(file, resolver)).toList();

    return new ApiSources(schemaSrc, opSrc);
  }

  @SneakyThrows
  private static ApiSource resolvePath(String file, ResourceResolver resolver) {
    var relativePath = Path.of(file);
    var absolutePath =
        resolver
            .resolveFile(relativePath)
            .orElseThrow(() -> new IllegalArgumentException("Failed to find file: " + file));

    return new ApiSource(relativePath, Files.readString(absolutePath));
  }

  public record ApiSources(ApiSource schema, List<ApiSource> operations) {

    public ApiSources(String inferredSchema) {
      this(new ApiSource(inferredSchema), List.of());
    }
  }
}
