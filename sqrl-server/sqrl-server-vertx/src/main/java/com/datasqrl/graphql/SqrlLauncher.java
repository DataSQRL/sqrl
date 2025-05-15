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

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;

/** Main entry point for launching the Vert.x application with Prometheus metrics. */
public class SqrlLauncher extends Launcher {

  public static void main(String[] args) {
    new SqrlLauncher().dispatch(args);
  }

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    var prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metricsOptions =
        new MicrometerMetricsOptions()
            .setMicrometerRegistry(prometheusMeterRegistry)
            .setEnabled(true);
    options.setMetricsOptions(metricsOptions);
  }
}
