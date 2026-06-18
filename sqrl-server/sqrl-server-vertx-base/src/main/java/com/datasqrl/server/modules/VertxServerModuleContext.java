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
package com.datasqrl.server.modules;

import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public record VertxServerModuleContext(
    Vertx vertx,
    Router router,
    ServerConfig config,
    Map<String, RootGraphQLModel> models,
    Path planDir,
    List<AutoCloseable> closeables)
    implements ServerModuleContext {}
