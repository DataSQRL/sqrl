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

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.kafka.KafkaSinkProducer;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.MutationComputedColumnType;
import com.datasqrl.graphql.server.MutationConfiguration;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoordsVisitor;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Purpose: Configures data fetchers for GraphQL mutations and executes the mutations (kafka
 * messages sending and SQL inserting) Collaboration: Uses {@link RootGraphqlModel} to get mutation
 * coordinates and creates data fetchers for Kafka and PostgreSQL.
 */
@Slf4j
@AllArgsConstructor
public class MutationConfigurationImpl implements MutationConfiguration<DataFetcher<?>> {

  private RootGraphqlModel root;
  private Vertx vertx;
  private ServerConfig config;

  @Override
  public MutationCoordsVisitor<DataFetcher<?>, Context> createSinkFetcherVisitor() {
    return new MutationCoordsVisitor<>() {
      @Override
      public DataFetcher<?> visit(KafkaMutationCoords coords, Context context) {
        KafkaProducer<String, String> producer =
            KafkaProducer.create(vertx, getSinkConfig(coords.isTransactional()));
        SinkProducer emitter = new KafkaSinkProducer<>(coords.getTopic(), producer);
        final List<String> uuidColumns =
            coords.getComputedColumns().entrySet().stream()
                .filter(e -> e.getValue() == MutationComputedColumnType.UUID)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
        final List<String> timestampColumns =
            coords.getComputedColumns().entrySet().stream()
                .filter(e -> e.getValue() == MutationComputedColumnType.TIMESTAMP)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());

        Preconditions.checkNotNull(
            emitter, "Could not find sink for field: %s", coords.getFieldName());
        return env -> {
          var entries = getEntries(env, uuidColumns);
          var cf = new CompletableFuture<Object>();

          Future<List<Object>> sendFuture =
              coords.isTransactional()
                  ? sendMessagesTransactionally(producer, entries, emitter, timestampColumns)
                  : sendMessagesNonTransactionally(
                      createSendFutures(entries, emitter, timestampColumns));

          sendFuture
              .onSuccess(results -> completeWithResults(cf, results, coords.isReturnList()))
              .onFailure(cf::completeExceptionally);

          return cf;
        };
      }
    };
  }

  private List<Map> getEntries(DataFetchingEnvironment env, List<String> uuidColumns) {
    // Rules:
    // - Only one argument is allowed, it doesn't matter the name
    // - input argument cannot be null.
    var args = env.getArguments();

    var argument = args.entrySet().stream().findFirst().map(Entry::getValue).get();

    List<Map> entries;
    if (argument instanceof List) {
      entries = (List<Map>) argument;
    } else {
      entries = List.of((Map) argument);
    }

    if (!uuidColumns.isEmpty()) {
      // Add UUID for event for the computed uuid columns
      entries.forEach(
          entry -> {
            var uuid = UUID.randomUUID();
            uuidColumns.forEach(colName -> entry.put(colName, uuid));
          });
    }
    return entries;
  }

  private List<Future<Map>> createSendFutures(
      List<Map> entries, SinkProducer emitter, List<String> timestampColumns) {
    return entries.stream()
        .map(
            entry ->
                emitter
                    .send(entry)
                    .map(
                        sinkResult -> {
                          // Add timestamp from sink to result
                          var dateTime =
                              ZonedDateTime.ofInstant(sinkResult.getSourceTime(), ZoneOffset.UTC);
                          timestampColumns.forEach(
                              colName -> entry.put(colName, dateTime.toOffsetDateTime()));
                          return entry;
                        }))
        .collect(Collectors.toList());
  }

  private void completeWithResults(
      CompletableFuture<Object> cf, List<Object> results, boolean returnList) {
    if (returnList) {
      cf.complete(results);
    } else {
      cf.complete(results.get(0));
    }
  }

  private Future<List<Object>> sendMessagesTransactionally(
      KafkaProducer<String, String> producer,
      List<Map> entries,
      SinkProducer emitter,
      List<String> timestampColumns) {
    return producer
        .initTransactions()
        .compose(v -> producer.beginTransaction())
        .compose(
            v -> {
              // Create send futures only after transaction is initialized and begun
              var futures = createSendFutures(entries, emitter, timestampColumns);
              return Future.join(futures);
            })
        .compose(compositeFuture -> producer.commitTransaction().map(v -> compositeFuture.list()))
        .recover(
            throwable -> producer.abortTransaction().compose(v -> Future.failedFuture(throwable)));
  }

  private Future<List<Object>> sendMessagesNonTransactionally(List<Future<Map>> futures) {
    return Future.join(futures).map(CompositeFuture::list);
  }

  // TODO: shouldn't it come from ServerConfig?
  Map<String, String> getSinkConfig(boolean transactional) {
    String clientUUID = UUID.randomUUID().toString();
    Map<String, String> conf = new HashMap<>();
    conf.put(BOOTSTRAP_SERVERS_CONFIG, config.getSystemProperty("PROPERTIES_BOOTSTRAP_SERVERS"));
    conf.put(KEY_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");
    conf.put(VALUE_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");

    if (transactional) {
      conf.put(TRANSACTIONAL_ID_CONFIG, "sqrl-mutation-" + clientUUID);
      conf.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
    }

    return conf;
  }
}
