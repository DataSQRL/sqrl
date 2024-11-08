package com.datasqrl.graphql;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.graphql.config.ServerConfig;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.kafka.KafkaSinkProducer;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.MutationConfiguration;
import com.datasqrl.graphql.server.RootGraphqlModel;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoordsVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresLogMutationCoords;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Vertx;
import io.vertx.ext.web.handler.graphql.schema.VertxDataFetcher;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

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
        Map<String, SinkProducer> sinks = new HashMap<>();
        for (MutationCoords mut : root.getMutations()) {
          if (mut instanceof KafkaMutationCoords) {
            KafkaMutationCoords kafkaMut = (KafkaMutationCoords) mut;
            KafkaProducer<String, String> producer = KafkaProducer.create(vertx, getSinkConfig());
            KafkaSinkProducer sinkProducer = new KafkaSinkProducer<>(kafkaMut.getTopic(), producer);
            sinks.put(mut.getFieldName(), sinkProducer);
          }
        }

        SinkProducer emitter = sinks.get(coords.getFieldName());

        Preconditions.checkNotNull(emitter, "Could not find sink for field: %s", coords.getFieldName());
        return VertxDataFetcher.create((env, fut) -> {

          Map entry = getEntry(env);

          emitter.send(entry)
              .onSuccess(sinkResult->{
                //Add timestamp from sink to result
                ZonedDateTime dateTime = ZonedDateTime.ofInstant(sinkResult.getSourceTime(), ZoneOffset.UTC);
                entry.put(ReservedName.MUTATION_TIME.getCanonical(), dateTime.toLocalDateTime());

                fut.complete(entry);
              })
              .onFailure((m)->
                  fut.fail(m)
              );
        });
      }

      @Override
      public DataFetcher<?> visit(PostgresLogMutationCoords coords, Context context) {
        return VertxDataFetcher.create((env, fut) -> {
          Map entry = getEntry(env);
          entry.put("event_time", Timestamp.from(Instant.now())); // TODO: better to do it in the db

          Object[] paramObj = new Object[coords.getParameters().size()];
          for (int i = 0; i < coords.getParameters().size(); i++) {
            String param = coords.getParameters().get(i);
            Object o = entry.get(param);
            if (o instanceof UUID) {
              o = ((UUID)o).toString();
            } else if (o instanceof Timestamp) {
              o = ((Timestamp) o).toLocalDateTime().atOffset(ZoneOffset.UTC);
            }
            paramObj[i] = o;
          }

          String insertStatement = coords.getInsertStatement();

          PreparedQuery<RowSet<Row>> preparedQuery = ((VertxJdbcClient) context.getClient())
              .getClients().get("postgres")
              .preparedQuery(insertStatement);
          preparedQuery.execute(Tuple.from(paramObj))
              .onComplete(e -> fut.complete(entry))
              .onFailure(e -> log.error("An error happened while executing the query: " + insertStatement, e));
        });
      }
    };
  }

  private Map getEntry(DataFetchingEnvironment env) {
    //Rules:
    //- Only one argument is allowed, it doesn't matter the name
    //- input argument cannot be null.
    Map<String, Object> args = env.getArguments();

    Map entry = (Map)args.entrySet().stream()
        .findFirst().map(Entry::getValue).get();

    //Add UUID for event
    UUID uuid = UUID.randomUUID();
    entry.put(ReservedName.MUTATION_PRIMARY_KEY.getDisplay(), uuid);
    return entry;
  }

  // TODO: shouldn't it come from ServerConfig?
  Map<String, String> getSinkConfig() {
    Map<String, String> conf = new HashMap<>();
    conf.put(BOOTSTRAP_SERVERS_CONFIG, config.getEnvironmentVariable("PROPERTIES_BOOTSTRAP_SERVERS"));
    conf.put(GROUP_ID_CONFIG, UUID.randomUUID().toString());
    conf.put(KEY_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");
    conf.put(VALUE_SERIALIZER_CLASS_CONFIG, "com.datasqrl.graphql.kafka.JsonSerializer");

    return conf;
  }

}
