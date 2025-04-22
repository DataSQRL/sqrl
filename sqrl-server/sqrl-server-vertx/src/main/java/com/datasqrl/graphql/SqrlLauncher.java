package com.datasqrl.graphql;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.core.Launcher;
import io.vertx.core.VertxOptions;
import io.vertx.micrometer.MicrometerMetricsOptions;

/**
 * Main entry point for launching the Vert.x application with Prometheus metrics.
 */
public class SqrlLauncher extends Launcher {

  public static void main(String[] args) {
    new SqrlLauncher().dispatch(args);
  }

  @Override
  public void beforeStartingVertx(VertxOptions options) {
    var prometheusMeterRegistry = new PrometheusMeterRegistry(
        PrometheusConfig.DEFAULT);
    var metricsOptions = new MicrometerMetricsOptions()
        .setMicrometerRegistry(prometheusMeterRegistry)
        .setEnabled(true);
    options.setMetricsOptions(metricsOptions);
  }
}
