package com.datasqrl.graphql;

import com.datasqrl.graphql.VertxJdbcClient.PreparedSqrlQueryImpl;
import com.datasqrl.graphql.server.Model.ArgumentParameter;
import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.ParameterHandlerVisitor;
import com.datasqrl.graphql.server.Model.JdbcParameterHandler;
import com.datasqrl.graphql.server.Model.ResolvedPagedJdbcQuery;
import com.datasqrl.graphql.server.Model.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.Model.SourceParameter;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.BuildGraphQLEngine;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.Value;

@Value
public class VertxQueryExecutionContext implements QueryExecutionContext,
    ParameterHandlerVisitor<Object, QueryExecutionContext> {
  VertxContext context;
  DataFetchingEnvironment environment;
  Set<FixedArgument> arguments;
  Promise<Object> fut;

  @Override
  public CompletableFuture runQuery(BuildGraphQLEngine server, ResolvedJdbcQuery pgQuery,
      boolean isList) {
    Object[] paramObj = new Object[pgQuery.getQuery().getParameters().size()];
    for (int i = 0; i < pgQuery.getQuery().getParameters().size(); i++) {
      JdbcParameterHandler param = pgQuery.getQuery().getParameters().get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }

    ((PreparedSqrlQueryImpl) pgQuery.getPreparedQueryContainer())
        .getPreparedQuery().execute(Tuple.from(paramObj))
        .map(r -> resultMapper(r, isList))
        .onSuccess(fut::complete)
        .onFailure(f -> {
          f.printStackTrace();
          fut.fail(f);
        });
    return null;
  }

  @Override
  public CompletableFuture runPagedJdbcQuery(ResolvedPagedJdbcQuery pgQuery,
      boolean isList, QueryExecutionContext context) {
    Optional<Integer> limit = Optional.ofNullable(getEnvironment().getArgument("limit"));
    Optional<Integer> offset = Optional.ofNullable(getEnvironment().getArgument("offset"));
    Object[] paramObj = new Object[pgQuery.getQuery().getParameters().size()];
    for (int i = 0; i < pgQuery.getQuery().getParameters().size(); i++) {
      JdbcParameterHandler param = pgQuery.getQuery().getParameters().get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }

    //Add limit + offset
    final String query = String.format("SELECT * FROM (%s) x LIMIT %s OFFSET %s",
        pgQuery.getQuery().getSql(),
        limit.map(Object::toString).orElse("ALL"),
        offset.orElse(0)
    );

    this.context.getSqlClient()
        .getSqlClient()
        .preparedQuery(query)
        .execute(Tuple.from(paramObj))
        .map(r -> resultMapper(r, isList))
        .onSuccess(fut::complete)
        .onFailure(f -> {
          f.printStackTrace();
          fut.fail(f);
        });
    return null;
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
