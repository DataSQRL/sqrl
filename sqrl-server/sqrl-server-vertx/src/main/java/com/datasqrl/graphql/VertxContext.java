package com.datasqrl.graphql;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.graphql.io.SinkConsumer;
import com.datasqrl.graphql.io.SinkProducer;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.JdbcClient;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcParameterHandler;
import com.datasqrl.graphql.server.RootGraphqlModel.KafkaMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoordsVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.PostgresLogMutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.VariableArgument;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.google.common.base.Preconditions;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.PropertyDataFetcher;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.graphql.schema.VertxDataFetcher;
import io.vertx.ext.web.handler.graphql.schema.VertxPropertyDataFetcher;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.SneakyThrows;
import lombok.Value;
import org.apache.calcite.jdbc.CalciteConnection;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

@Value
public class VertxContext implements Context {

  private static final Logger log = LoggerFactory.getLogger(VertxContext.class);
  VertxJdbcClient sqlClient;
  Map<String, SinkProducer> sinks;
  Map<String, SinkConsumer> subscriptions;
  NameCanonicalizer canonicalizer;

  @Override
  public JdbcClient getClient() {
    return sqlClient;
  }

  @Override
  public DataFetcher<Object> createPropertyFetcher(String name) {
    return VertxCreateCaseInsensitivePropertyDataFetcher.createCaseInsensitive(name);
  }
  public interface VertxCreateCaseInsensitivePropertyDataFetcher {

    static PropertyDataFetcher<Object> createCaseInsensitive(String propertyName) {
      return new PropertyDataFetcher<Object>(propertyName) {
        @Override
        public Object get(DataFetchingEnvironment environment) {
          Object source = environment.getSource();
          if (source instanceof JsonObject) {
            JsonObject jsonObject = (JsonObject) source;
            Object value = jsonObject.getValue(getPropertyName());
            if (value != null) {
              return value;
            }
            // bad hack, remove me
            return jsonObject.getMap().entrySet().stream()
                .filter(e->e.getKey().equalsIgnoreCase(getPropertyName()))
                .map(e->e.getValue())
                .findAny()
                .orElse(null);
          }
          return super.get(environment);
        }
      };
    }
  }
  @Override
  public DataFetcher<?> createArgumentLookupFetcher(GraphQLEngineBuilder server, Map<Set<Argument>, ResolvedQuery> lookupMap) {
    return VertxDataFetcher.create((env, fut) -> {
      //Map args
      Set<Argument> argumentSet = env.getArguments().entrySet().stream()
          .map(argument -> new VariableArgument(argument.getKey(), argument.getValue()))
          .collect(Collectors.toSet());

      //Find query
      ResolvedQuery resolvedQuery = lookupMap.get(argumentSet);
      if (resolvedQuery == null) {
        fut.fail("Could not find query: " + env.getArguments());
        return;
      }
      //Execute
      QueryExecutionContext context = new VertxQueryExecutionContext(this,
          env, argumentSet, fut);
      resolvedQuery.accept(server, context);
    });
  }

  @Override
  public MutationCoordsVisitor createSinkFetcherVisitor() {
    return new MutationCoordsVisitor() {
      @Override
      public DataFetcher<?> visit(KafkaMutationCoords coords) {
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
      public DataFetcher<?> visit(PostgresLogMutationCoords coords) {

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
              o = ((Timestamp) o).toLocalDateTime();
            }
            paramObj[i] = o;
          }

          PreparedQuery<RowSet<Row>> preparedQuery = sqlClient.getClients().get("postgres")
              .preparedQuery(coords.getInsertStatement());
          preparedQuery.execute(Tuple.from(paramObj))
              .onComplete(e -> fut.complete(entry))
              .onFailure(e -> {
                e.printStackTrace();
                fut.fail(e);
              });
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

  @Override
  public DataFetcher<?> createSubscriptionFetcher(SubscriptionCoords coords, Map<String, String> filters) {
    SinkConsumer consumer = subscriptions.get(coords.getFieldName());
    Preconditions.checkNotNull(consumer, "Could not find subscription consumer: {}", coords.getFieldName());

    Flux<Object> deferredFlux = Flux.<Object>create(sink ->
        consumer.listen(sink::next, sink::error, (x) -> sink.complete())).share();

    return new DataFetcher<>() {
      @Override
      public Publisher<Object> get(DataFetchingEnvironment env) throws Exception {
        return deferredFlux.filter(entry -> !filterSubscription(entry, env.getArguments()));
      }

      private boolean filterSubscription(Object data, Map<String, Object> args) {
        if (args == null) {
          return false;
        }
        for (Map.Entry<String, String> filter : filters.entrySet()) {
          Object argValue = args.get(filter.getKey());
          if (argValue == null) continue;

          Map<String, Object> objectMap;
          if (data instanceof Map) {
            objectMap = (Map) data;
          } else if (data instanceof JsonObject) {
            objectMap = ((JsonObject)data).getMap();
          } else {
            objectMap = Map.of();
          }

          Object retrievedData = objectMap.get(filter.getValue());
          if (!argValue.equals(retrievedData)) {
            return true;
          }
        }

        return false;
      }
    };
  }
}
