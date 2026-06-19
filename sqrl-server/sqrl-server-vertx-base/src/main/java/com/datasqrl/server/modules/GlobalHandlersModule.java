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

import com.datasqrl.server.DetailedRequestTracer;
import com.datasqrl.server.config.CorsHandlerOptions;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.CorsHandler;
import io.vertx.ext.web.handler.LoggerHandler;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;

public class GlobalHandlersModule implements ServerModule<VertxServerModuleContext> {

  @Override
  public CompletionStage<Void> configure(VertxServerModuleContext ctx) {
    var router = ctx.router();

    router.route().handler(toCorsHandler(ctx.config().getCorsHandlerOptions()));
    router.route().handler(BodyHandler.create());

    // Detailed tracing reads request bodies, so it must remain after BodyHandler.
    if (System.getenv("SQRL_DEBUG") != null) {
      router.route().handler(new DetailedRequestTracer());
    } else {
      router.route().handler(LoggerHandler.create());
    }

    return CompletableFuture.completedFuture(null);
  }

  private CorsHandler toCorsHandler(CorsHandlerOptions opts) {
    var ch =
        opts.getAllowedOrigin() != null
            ? CorsHandler.create().addOrigin(opts.getAllowedOrigin())
            : CorsHandler.create();

    if (opts.getAllowedOrigins() != null) {
      ch.addOrigins(opts.getAllowedOrigins());
    }

    return ch.allowedMethods(
            opts.getAllowedMethods().stream().map(HttpMethod::valueOf).collect(Collectors.toSet()))
        .allowedHeaders(opts.getAllowedHeaders())
        .exposedHeaders(opts.getExposedHeaders())
        .allowCredentials(opts.isAllowCredentials())
        .maxAgeSeconds(opts.getMaxAgeSeconds())
        .allowPrivateNetwork(opts.isAllowPrivateNetwork());
  }
}
