package com.datasqrl.graphql;

import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;

import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.kafka.KafkaDataFetcherFactory;
import com.datasqrl.graphql.kafka.KafkaSinkConsumer;
import com.datasqrl.graphql.postgres_log.PostgresDataFetcherFactory;
import com.datasqrl.graphql.postgres_log.PostgresListenNotifyConsumer;
import com.datasqrl.graphql.postgres_log.PostgresSinkConsumer;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoordsVisitor;
import com.datasqrl.graphql.server.SubscriptionConfiguration;
import graphql.schema.DataFetcher;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AllArgsConstructor
public class SubscriptionConfigurationImpl implements SubscriptionConfiguration<DataFetcher<?>> {

  RootGraphqlModel root;
  Vertx vertx;
  ServerConfig config;
  Promise<Void> startPromise;
  VertxJdbcClient client;

  @Override
  public SubscriptionCoordsVisitor<DataFetcher<?>, Context> createSubscriptionFetcherVisitor() {
    return new SubscriptionCoordsVisitor<>() {
      @Override
      public DataFetcher<?> visit(KafkaSubscriptionCoords coords, Context context) {
        Map<String, SinkConsumer> subscriptions = new HashMap<>();
        for (SubscriptionCoords sub: root.getSubscriptions()) {
          KafkaSubscriptionCoords kafkaSub = (KafkaSubscriptionCoords) sub;
          KafkaConsumer<String, String> consumer = KafkaConsumer.create(vertx, getSourceConfig());
          consumer.subscribe(kafkaSub.getTopic())
              .onSuccess(v -> log.info("Subscribed to topic: {}", kafkaSub.getTopic()))
              .onFailure(startPromise::fail);

          subscriptions.put(sub.getFieldName(), new KafkaSinkConsumer<>(consumer));
        }
        return KafkaDataFetcherFactory.create(subscriptions, coords);
      }

      @Override
      public DataFetcher<?> visit(PostgresSubscriptionCoords coords, Context context) {
        Map<String, SinkConsumer> subscriptions = new HashMap<>();
        for (SubscriptionCoords sub: root.getSubscriptions()) {
          PostgresSubscriptionCoords pgSub = (PostgresSubscriptionCoords) sub;
          PostgresListenNotifyConsumer pgConsumer = new PostgresListenNotifyConsumer(client,
              pgSub.getListenQuery(), pgSub.getOnNotifyQuery(), pgSub.getParameters(), vertx,
              config.getPgConnectOptions());

          PostgresSinkConsumer pgSinkConsumer = new PostgresSinkConsumer(pgConsumer);

          subscriptions.put(pgSub.getFieldName(), pgSinkConsumer);
        }
        return PostgresDataFetcherFactory.create(subscriptions, coords);
      }
    };
  }

  // TODO: shouldn't it come from ServerConfig all together?
  public Map<String, String> getSourceConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.put(BOOTSTRAP_SERVERS_CONFIG, config.getEnvironmentVariable("PROPERTIES_BOOTSTRAP_SERVERS"));
    conf.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
    conf.put(KEY_DESERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonDeserializer");
    conf.put(VALUE_DESERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonDeserializer");
    conf.put(AUTO_OFFSET_RESET_CONFIG, "latest");
    return conf;
  }
}
