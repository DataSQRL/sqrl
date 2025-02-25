package com.datasqrl.graphql;

import static com.datasqrl.graphql.VertxJdbcClient.getDatabaseName;
import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.graphql.VertxJdbcClient.PreparedSqrlQueryImpl;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcParameterHandler;
import com.datasqrl.graphql.server.RootGraphqlModel.ParameterHandlerVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedPagedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SourceParameter;
import graphql.schema.DataFetchingEnvironment;
import io.micrometer.core.instrument.Counter;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.Value;
import org.jetbrains.annotations.NotNull;

/**
 * It is the ExecutionContext per servlet type. It is responsible for executing the resolved SQL
 * queries (paginated or not) in Vert.x and mapping the database resultSet to json for using in
 * GraphQL responses. It also implements the parameters and arguments visitors for the {@link
 * com.datasqrl.graphql.server.RootGraphqlModel} visitors
 */
@Value
public class VertxQueryExecutionContext
    implements QueryExecutionContext, ParameterHandlerVisitor<Object, QueryExecutionContext> {
  VertxContext context;
  DataFetchingEnvironment environment;
  Set<Argument> arguments;
  Promise<Object> future; // basically Vert.x completableFuture

  @Override
  public CompletableFuture runQuery(GraphQLEngineBuilder server, ResolvedJdbcQuery pgQuery,
      boolean isList) {
    final Object[] paramObj = getParamArguments(pgQuery.getQuery().getParameters());

    String database = getDatabaseName(pgQuery.getQuery());

    // execute the preparedQuery with the arguments extracted above
    PreparedSqrlQueryImpl preparedQueryContainer = (PreparedSqrlQueryImpl) pgQuery.getPreparedQueryContainer();
    Future<RowSet<Row>> future;
    if (preparedQueryContainer == null) {
      future = this.context.getSqlClient().execute(database,
          pgQuery.getQuery().getSql(), Tuple.from(paramObj));
    } else {
      PreparedQuery<RowSet<Row>> preparedQuery = preparedQueryContainer
          .getPreparedQuery();
      future = this.context.getSqlClient().execute(database,
          preparedQuery, Tuple.from(paramObj));
    }
    // map the resultSet to json for GraphQL response
    future
        .map(r -> resultMapper(r, isList))
        .onSuccess(result -> this.future.complete(result))
        .onFailure(f -> {
          f.printStackTrace();
          this.future.fail(f);
        });
    return new CompletableFuture();
  }

  @Override
  public CompletableFuture runPagedJdbcQuery(ResolvedPagedJdbcQuery databaseQuery,
      boolean isList, QueryExecutionContext context) {
    Optional<Integer> limit = Optional.ofNullable(getEnvironment().getArgument(LIMIT));
    Optional<Integer> offset = Optional.ofNullable(getEnvironment().getArgument(OFFSET));
    final Object[] paramArguments = getParamArguments(databaseQuery.getQuery().getParameters());

    //Add limit + offset
    final String query = String.format("SELECT * FROM (%s) x LIMIT %s OFFSET %s",
        databaseQuery.getQuery().getSql(),
        limit.map(Object::toString).orElse("ALL"),
        offset.orElse(0)
    );

    String database = getDatabaseName(databaseQuery.getQuery());

    Future<RowSet<Row>> future = this.context.getSqlClient().execute(database,
        query,Tuple.from(paramArguments));

    future
      .map(r -> resultMapper(r, isList))
      .onSuccess(result -> this.future.complete(result))
      .onFailure(f -> {
        f.printStackTrace();
        this.future.fail(f);
      });

    return new CompletableFuture();
  }

  private Object @NotNull [] getParamArguments(List<JdbcParameterHandler> parameters) {
    Object[] paramObj = new Object[parameters.size()];
    for (int i = 0; i < parameters.size(); i++) {
      JdbcParameterHandler param = parameters.get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }
    return paramObj;
  }

  private Object resultMapper(RowSet<Row> r, boolean isList) {
    List<JsonObject> o = StreamSupport.stream(r.spliterator(), false)
        .map(Row::toJson)
        .collect(Collectors.toList());

    return isList
        ? o
        : (!o.isEmpty() ? o.get(0) : null);
  }

  @SneakyThrows
  @Override
  public Object visitSourceParameter(SourceParameter sourceParameter,
      QueryExecutionContext context) {
    return context.getContext().createPropertyFetcher(sourceParameter.getKey())
        .get(context.getEnvironment());
  }

  @Override
  public Object visitArgumentParameter(ArgumentParameter argumentParameter,
      QueryExecutionContext context) {
    return context.getArguments().stream()
        .filter(arg -> arg.getPath().equalsIgnoreCase(argumentParameter.getPath()))
        .findFirst()
        .map(Argument::getValue)
        .orElse(null);
  }
}
