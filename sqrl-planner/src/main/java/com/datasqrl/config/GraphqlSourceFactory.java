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

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.APISource;
import com.datasqrl.graphql.APISourceImpl;
import com.datasqrl.graphql.ScriptFiles;
import com.datasqrl.module.resolver.ResourceResolver;
import com.google.inject.Inject;
import java.util.Optional;

public class GraphqlSourceFactory {
  Optional<APISource> apiSchemaOpt;

  @Inject
  public GraphqlSourceFactory(
      ScriptFiles scriptFiles,
      NameCanonicalizer nameCanonicalizer,
      ResourceResolver resourceResolver) {
    apiSchemaOpt =
        scriptFiles
            .getConfig()
            .getGraphql()
            .map(file -> APISourceImpl.of(file, nameCanonicalizer, resourceResolver));
  }

  public Optional<APISource> get() {
    return apiSchemaOpt;
  }
}
