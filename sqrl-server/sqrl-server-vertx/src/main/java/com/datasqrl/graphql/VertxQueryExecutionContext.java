package com.datasqrl.graphql;

import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.graphql.VertxJdbcClient.PreparedSqrlQueryImpl;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.ParameterHandlerVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcParameterHandler;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedPagedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SourceParameter;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.Value;

@Value
public class VertxQueryExecutionContext implements QueryExecutionContext,
    ParameterHandlerVisitor<Object, QueryExecutionContext> {
  VertxContext context;
  DataFetchingEnvironment environment;
  Set<Argument> arguments;
  Promise<Object> fut;

  @Override
  public CompletableFuture runQuery(GraphQLEngineBuilder server, ResolvedJdbcQuery pgQuery,
      boolean isList) {
    Object[] paramObj = new Object[pgQuery.getQuery().getParameters().size()];
    for (int i = 0; i < pgQuery.getQuery().getParameters().size(); i++) {
      JdbcParameterHandler param = pgQuery.getQuery().getParameters().get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }

    SqlClient sqlClient = this.context.getSqlClient().getClients().get(pgQuery.getQuery().getDatabase());
    if (sqlClient == null) {
      throw new RuntimeException("Could not find database engine: " + pgQuery.getQuery().getDatabase());
    }

    sqlClient.query("INSTALL iceberg;").execute().compose(v ->
        sqlClient.query("LOAD iceberg;").execute()
    ).compose(v ->
        // Now you can proceed with your original prepared query

            ((PreparedSqrlQueryImpl) pgQuery.getPreparedQueryContainer())
                .getPreparedQuery().execute(Tuple.from(paramObj))
                .map(r -> resultMapper(r, isList))
    ) .onSuccess(fut::complete)
        .onFailure(f -> {
          f.printStackTrace();
          fut.fail(f);
        });

    return null;
  }

  @Override
  public CompletableFuture runPagedJdbcQuery(ResolvedPagedJdbcQuery databaseQuery,
      boolean isList, QueryExecutionContext context) {
    Optional<Integer> limit = Optional.ofNullable(getEnvironment().getArgument(LIMIT));
    Optional<Integer> offset = Optional.ofNullable(getEnvironment().getArgument(OFFSET));
    Object[] paramObj = new Object[databaseQuery.getQuery().getParameters().size()];
    for (int i = 0; i < databaseQuery.getQuery().getParameters().size(); i++) {
      JdbcParameterHandler param = databaseQuery.getQuery().getParameters().get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }

    //Add limit + offset
    final String query = String.format("SELECT * FROM (%s) x LIMIT %s OFFSET %s",
        databaseQuery.getQuery().getSql(),
        limit.map(Object::toString).orElse("ALL"),
        offset.orElse(0)
    );

    SqlClient sqlClient = this.context.getSqlClient().getClients().get(databaseQuery.getDatabase());
    if (sqlClient == null) {
      throw new RuntimeException("Could not find database engine: " + databaseQuery.getDatabase());
    }
    sqlClient.query("INSTALL iceberg;").execute().compose(v ->
            sqlClient.query("LOAD iceberg;").execute()
        ).compose(v ->
        // Now you can proceed with your original prepared query
        sqlClient.preparedQuery(query) .execute(Tuple.from(paramObj))
            .map(r -> resultMapper(r, isList))
    )
        .onSuccess(fut::complete)
        .onFailure(f -> {
          f.printStackTrace();
          fut.fail(f);
        });

    return new CompletableFuture();
  }

  private Object resultMapper(RowSet<Row> r, boolean isList) {
    List<JsonObject> o = StreamSupport.stream(r.spliterator(), false)
        .map(Row::toJson)
        .collect(Collectors.toList());

    return isList
        ? o
        : (o.size() > 0 ? o.get(0) : null);
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
        .map(f -> f.getValue())
        .orElse(null);
  }
}
