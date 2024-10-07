package com.datasqrl.graphql;

import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.kafka.KafkaDataFetcherFactory;
import com.datasqrl.graphql.postgres_log.PostgresDataFetcherFactory;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresSubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoordsVisitor;
import com.datasqrl.graphql.server.SubscriptionConfiguration;
import graphql.schema.DataFetcher;
import java.util.Map;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class SubscriptionConfigurationImpl implements SubscriptionConfiguration<DataFetcher<?>> {

  private Map<String, SinkConsumer> subscriptions;

  @Override
  public SubscriptionCoordsVisitor<DataFetcher<?>, Context> createSubscriptionFetcherVisitor() {
    return new SubscriptionCoordsVisitor<>() {
      @Override
      public DataFetcher<?> visit(KafkaSubscriptionCoords coords, Context context) {
        return KafkaDataFetcherFactory.create(subscriptions, coords);
      }

      @Override
      public DataFetcher<?> visit(PostgresSubscriptionCoords coords, Context context) {
        return PostgresDataFetcherFactory.create(subscriptions, coords);
      }
    };
  }
}
