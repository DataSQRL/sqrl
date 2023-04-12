package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.config.SinkFactory;
import com.datasqrl.config.SinkServiceLoader;
import com.datasqrl.config.SourceFactory;
import com.datasqrl.config.SourceServiceLoader;
import com.datasqrl.engine.stream.flink.FlinkEngineConfiguration;

public class FlinkConnectorServiceLoader {
  public static Class<?> resolveSourceClass(String connectorName) {
    SourceFactory<?> factory = (new SourceServiceLoader())
        .load(FlinkEngineConfiguration.ENGINE_NAME, connectorName)
        .orElseThrow();

    return factory.getClass();
  }

  public static Class<?> resolveSinkClass(String connectorName) {
    SinkFactory factory = (new SinkServiceLoader())
        .load(FlinkEngineConfiguration.ENGINE_NAME, connectorName)
        .orElseThrow();

    return factory.getClass();
  }

}
