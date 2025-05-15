package com.datasqrl.graphql;

import io.micrometer.prometheusmetrics.PrometheusConfig;
import io.micrometer.prometheusmetrics.PrometheusMeterRegistry;
import io.vertx.launcher.application.HookContext;
import io.vertx.launcher.application.VertxApplication;
import io.vertx.launcher.application.VertxApplicationHooks;
import io.vertx.micrometer.MicrometerMetricsFactory;

/**
 * Main entry point for launching the Vert.x application with Prometheus metrics.
 */
public class SqrlLauncher   implements VertxApplicationHooks{


  public static void main(String[] args) {
      VertxApplication vertxApplication = new VertxApplication(args, new SqrlLauncher());
      vertxApplication.launch();
  }

  
  @Override
  public void beforeStartingVertx(HookContext context) {
    var prometheusMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
    var metricsOptions = new MicrometerMetricsFactory(prometheusMeterRegistry)
              .newOptions()
              .setEnabled(true);
    context.vertxOptions().setMetricsOptions(metricsOptions);
  }
}
