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

import com.datasqrl.server.graphql.ModelUtils;
import com.datasqrl.server.kafka.KafkaHealthTracker;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.healthchecks.Status;
import io.vertx.ext.web.healthchecks.HealthCheckHandler;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

public class HealthChecksModule implements ServerModule<VertxServerModuleContext> {

  @Override
  public CompletionStage<Void> configure(VertxServerModuleContext ctx) {
    var healthCheckHandler = HealthCheckHandler.create(ctx.vertx());

    var kafkaHealthTrackerCtx = Optional.<KafkaHealthTracker.Context>empty();
    for (var model : ctx.models().values()) {
      var topic = ModelUtils.findFirstKafkaMutationTopic(model);
      if (topic.isPresent()) {
        kafkaHealthTrackerCtx =
            Optional.of(
                new KafkaHealthTracker.Context(
                    topic.get(), ctx.config().getKafkaMutationConfig().asMap(false)));
        break;
      }

      topic = ModelUtils.findFirstKafkaSubscriptionTopic(model);
      if (topic.isPresent()) {
        kafkaHealthTrackerCtx =
            Optional.of(
                new KafkaHealthTracker.Context(
                    topic.get(), ctx.config().getKafkaSubscriptionConfig().asMap()));
      }
    }

    if (kafkaHealthTrackerCtx.isPresent()) {
      var kafkaHealthTracker = new KafkaHealthTracker(ctx.vertx(), kafkaHealthTrackerCtx.get());
      ctx.closeables().add(kafkaHealthTracker);

      healthCheckHandler.register(
          "kafka",
          promise -> {
            if (kafkaHealthTracker.isHealthy()) {
              promise.complete(Status.OK());
            } else {
              promise.complete(
                  Status.KO(
                      new JsonObject()
                          .put("reason", "Kafka broker probe failed")
                          .put("error", kafkaHealthTracker.lastError())));
            }
          });
    }

    ctx.router().get("/health*").handler(healthCheckHandler);
    return CompletableFuture.completedFuture(null);
  }
}
