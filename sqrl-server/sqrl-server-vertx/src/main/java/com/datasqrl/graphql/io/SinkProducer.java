package com.datasqrl.graphql.io;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.Map;

public interface SinkProducer {
  public Future<SinkResult> send(Map entry);
}
