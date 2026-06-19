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
package com.datasqrl.server;

import com.datasqrl.server.config.ServerConfig;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.SubscriptionCoordsVisitor;
import com.datasqrl.server.kafka.KafkaDataFetcherFactory;
import com.datasqrl.server.kafka.KafkaSinkConsumer;
import graphql.schema.DataFetcher;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Purpose: Configures {@link DataFetcher}s for GraphQL subscriptions the subscriptions (kafka
 * messages subscription and postgreSQL listen/notify mechanism) that embed the code for executing
 * the subscriptions.
 *
 * <p>Collaboration: Uses {@link RootGraphQLModel} to get subscription coordinates and create data
 * fetchers for Kafka and PostgreSQL.
 */
@Slf4j
@RequiredArgsConstructor
public class SubscriptionConfigurationImpl implements SubscriptionConfiguration<DataFetcher<?>> {

  private final Vertx vertx;
  private final ServerConfig config;
  private final List<Future<Void>> subscriptionFutures = new ArrayList<>();

  @Override
  public SubscriptionCoordsVisitor<DataFetcher<?>, ServerContext>
      createSubscriptionFetcherVisitor() {
    return (coords, context) -> {
      KafkaConsumer<String, String> consumer =
          KafkaConsumer.create(vertx, config.getKafkaSubscriptionConfig().asMap());
      var subscriptionFuture =
          consumer
              .subscribe(coords.getTopic())
              .onSuccess(v -> log.info("Subscribed to topic: {}", coords.getTopic()))
              .onFailure(
                  err -> log.error("Failed to subscribe to topic: {}", coords.getTopic(), err));
      subscriptionFutures.add(subscriptionFuture);
      return KafkaDataFetcherFactory.create(new KafkaSinkConsumer<>(consumer), coords, context);
    };
  }

  /**
   * Returns a composite future that completes when all subscriptions have been set up successfully,
   * or fails if any subscription fails.
   *
   * @return a future that tracks all subscription setups
   */
  public Future<Void> getAllSubscriptionsFuture() {
    return subscriptionFutures.isEmpty()
        ? Future.succeededFuture()
        : Future.all(subscriptionFutures).mapEmpty();
  }
}
