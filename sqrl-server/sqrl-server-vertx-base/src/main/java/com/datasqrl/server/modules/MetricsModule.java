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

import com.google.common.collect.MoreCollectors;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.micrometer.backends.BackendRegistries;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetricsModule implements ServerModule<VertxServerModuleContext> {

  @Override
  public CompletionStage<Void> configure(VertxServerModuleContext context) {
    var meterRegistry = findMeterRegistry();
    meterRegistry.ifPresent(
        registry -> {
          registerJvmMetrics(context, registry);

          context
              .router()
              .route("/metrics")
              .handler(
                  ctx ->
                      ctx.response()
                          .putHeader("content-type", "text/plain")
                          .end(registry.scrape()));
        });

    return CompletableFuture.completedFuture(null);
  }

  private Optional<PrometheusMeterRegistry> findMeterRegistry() {
    var registry = BackendRegistries.getDefaultNow();
    if (registry == null) {
      return Optional.empty();
    }

    log.info("Found registry: {}", registry.getClass().getSimpleName());

    if (registry instanceof PrometheusMeterRegistry meterRegistry) {
      return Optional.of(meterRegistry);
    }

    if (registry instanceof CompositeMeterRegistry compositeRegistry) {
      return compositeRegistry.getRegistries().stream()
          .filter(PrometheusMeterRegistry.class::isInstance)
          .map(PrometheusMeterRegistry.class::cast)
          .collect(MoreCollectors.toOptional());
    }

    throw new IllegalStateException(
        "Unable to register metrics to: " + registry.getClass().getSimpleName());
  }

  private void registerJvmMetrics(
      VertxServerModuleContext context, PrometheusMeterRegistry registry) {
    new UptimeMetrics().bindTo(registry);
    new JvmMemoryMetrics().bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);

    var jvmGcMetrics = new JvmGcMetrics();
    jvmGcMetrics.bindTo(registry);
    context.closeables().add(jvmGcMetrics);
  }
}
