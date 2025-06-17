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
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
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
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresLogMutationCoords;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.sqlclient.Tuple;
import java.sql.Timestamp;
import java.time.Instant;
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
        KafkaProducer<String, String> producer = KafkaProducer.create(vertx, getSinkConfig());
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
        DataFetcher<?> dataFetcher =
            env -> {
              var entry = getEntry(env, uuidColumns);

              var cf = new CompletableFuture<Object>();
              emitter
                  .send(entry)
                  .onSuccess(
                      sinkResult -> {
                        // Add timestamp from sink to result
                        var dateTime =
                            ZonedDateTime.ofInstant(sinkResult.getSourceTime(), ZoneOffset.UTC);
                        timestampColumns.forEach(
                            colName -> entry.put(colName, dateTime.toOffsetDateTime()));

                        cf.complete(entry);
                      })
                  .onFailure((m) -> cf.completeExceptionally(m));

              return cf;
            };

        return dataFetcher;
      }

      @Override
      public DataFetcher<?> visit(PostgresLogMutationCoords coords, Context context) {
        DataFetcher<?> dataFetcher =
            env -> {
              var entry = getEntry(env, List.of());
              entry.put(
                  "event_time", Timestamp.from(Instant.now())); // TODO: better to do it in the db

              var paramObj = new Object[coords.getParameters().size()];
              for (var i = 0; i < coords.getParameters().size(); i++) {
                var param = coords.getParameters().get(i);
                var o = entry.get(param);
                if (o instanceof UUID iD) {
                  o = iD.toString();
                } else if (o instanceof Timestamp timestamp) {
                  o = timestamp.toLocalDateTime().atOffset(ZoneOffset.UTC);
                }
                paramObj[i] = o;
              }

              var insertStatement = coords.getInsertStatement();

              var preparedQuery =
                  ((VertxJdbcClient) context.getClient())
                      .getClients()
                      .get("postgres")
                      .preparedQuery(insertStatement);

              var cf = new CompletableFuture<Object>();

              preparedQuery
                  .execute(Tuple.from(paramObj))
                  .onComplete(e -> cf.complete(entry))
                  .onFailure(
                      e ->
                          log.error(
                              "An error happened while executing the query: " + insertStatement,
                              e));

              return cf;
            };

        return dataFetcher;
      }
    };
  }

  private Map getEntry(DataFetchingEnvironment env, List<String> uuidColumns) {
    // Rules:
    // - Only one argument is allowed, it doesn't matter the name
    // - input argument cannot be null.
    var args = env.getArguments();

    var entry = (Map) args.entrySet().stream().findFirst().map(Entry::getValue).get();

    if (!uuidColumns.isEmpty()) {
      // Add UUID for event for the computed uuid columns
      var uuid = UUID.randomUUID();
      uuidColumns.forEach(colName -> entry.put(colName, uuid));
    }
    return entry;
  }

  // TODO: shouldn't it come from ServerConfig?
  Map<String, String> getSinkConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.put(
        BOOTSTRAP_SERVERS_CONFIG, config.getEnvironmentVariable("PROPERTIES_BOOTSTRAP_SERVERS"));
    conf.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
    conf.put(KEY_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");
    conf.put(VALUE_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");

    return conf;
  }
}
