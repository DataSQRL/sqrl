package com.datasqrl.graphql.io;

import java.util.Map;

import io.vertx.core.Future;

/**
 *  Sending records to a sink (such as a Kafka topic)
 */

public interface SinkProducer {
  public Future<SinkResult> send(Map entry);
}
