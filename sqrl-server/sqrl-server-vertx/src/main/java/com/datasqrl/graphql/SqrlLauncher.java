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
package com.datasqrl.graphql;

import com.datasqrl.env.GlobalEnvironmentStore;
import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.launcher.application.HookContext;
import io.vertx.launcher.application.VertxApplication;
import io.vertx.launcher.application.VertxApplicationHooks;
import io.vertx.micrometer.MicrometerMetricsFactory;

/** Main entry point for launching the Vert.x application with Prometheus metrics. */
public class SqrlLauncher implements VertxApplicationHooks {

  public static void main(String[] args) {
    GlobalEnvironmentStore.putAll(System.getenv());

    if (args == null || args.length == 0) {
      args = new String[] {HttpServerVerticle.class.getName()};
    }
    VertxApplication vertxApplication = new VertxApplication(args, new SqrlLauncher());
    vertxApplication.launch();
  }

  @Override
  public void beforeStartingVertx(HookContext context) {
    var prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    // Register the Prometheus registry with Micrometer's global registries
    io.micrometer.core.instrument.Metrics.addRegistry(prometheusMeterRegistry);
    var metricsOptions =
        new MicrometerMetricsFactory(prometheusMeterRegistry).newOptions().setEnabled(true);
    context.vertxOptions().setMetricsOptions(metricsOptions);
  }
}
