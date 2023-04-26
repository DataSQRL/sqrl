package com.datasqrl.engine.stream.flink.sql;

import com.datasqrl.config.*;
import com.datasqrl.engine.stream.flink.FlinkEngineFactory;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import lombok.Getter;

public class FlinkConnectorServiceLoader {

  public static<C extends FlinkSourceFactory> C getSourceFactory(String connectorName, Class<C> sourceClass) {
    Predicate<SourceFactory> selector = c -> c.getEngine().equalsIgnoreCase(FlinkEngineFactory.ENGINE_NAME)
            && c.getSourceName().equalsIgnoreCase(connectorName)
            && sourceClass.isInstance(c);
    return sourceClass.cast(ServiceLoaderDiscovery.get(SourceFactory.class, selector,
      List.of(FlinkEngineFactory.ENGINE_NAME, connectorName, sourceClass.getSimpleName())));
  }

  public static<C extends FlinkSourceFactory> Class<? extends FlinkSourceFactory> resolveSourceClass(String connectorName, Class<C> sourceClass) {
    return getSourceFactory(connectorName, sourceClass).getClass();
  }

  public static TableDescriptorSinkFactory getSinkFactory(String connectorName) {
    Predicate<SinkFactory> selector = c -> c.getEngine().equalsIgnoreCase(FlinkEngineFactory.ENGINE_NAME)
            && c.getSinkType().equalsIgnoreCase(connectorName);
    return (TableDescriptorSinkFactory)ServiceLoaderDiscovery.get(SinkFactory.class, selector,
            List.of(FlinkEngineFactory.ENGINE_NAME, connectorName));
  }

  public static Class<? extends SinkFactory> resolveSinkClass(String connectorName) {
    return getSinkFactory(connectorName).getClass();
  }

}
