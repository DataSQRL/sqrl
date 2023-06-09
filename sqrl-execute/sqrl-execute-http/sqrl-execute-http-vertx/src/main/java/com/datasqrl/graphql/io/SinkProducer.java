package com.datasqrl.graphql.io;

import io.vertx.core.Future;
import java.util.Map;

public interface SinkProducer {
  public Future<SinkResult> send(Map<String, Object> entry);
}
