package com.datasqrl.graphql;

import com.datasqrl.graphql.VertxJdbcClient.PreparedSqrlQueryImpl;
import com.datasqrl.graphql.server.Model.ArgumentPgParameter;
import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.ParameterHandlerVisitor;
import com.datasqrl.graphql.server.Model.PgParameterHandler;
import com.datasqrl.graphql.server.Model.ResolvedPagedPgQuery;
import com.datasqrl.graphql.server.Model.ResolvedPgQuery;
import com.datasqrl.graphql.server.Model.SourcePgParameter;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.SqrlGraphQLServer;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;

@Value
@Slf4j
public class VertxQueryExecutionContext implements QueryExecutionContext,
    ParameterHandlerVisitor<Object, QueryExecutionContext> {
  VertxContext context;
  DataFetchingEnvironment environment;
  Set<FixedArgument> arguments;
  Promise<Object> fut;

  @Override
  public CompletableFuture runQuery(SqrlGraphQLServer server, ResolvedPgQuery pgQuery,
      boolean isList) {
    Object[] paramObj = new Object[pgQuery.getQuery().getParameters().size()];
    for (int i = 0; i < pgQuery.getQuery().getParameters().size(); i++) {
      PgParameterHandler param = pgQuery.getQuery().getParameters().get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }

    log.info("Query: " + ((PreparedSqrlQueryImpl) pgQuery.getPreparedQueryContainer())
        .getPreparedQuery() + "\n Params: "+ Arrays.toString(paramObj));

    ((PreparedSqrlQueryImpl) pgQuery.getPreparedQueryContainer())
        .getPreparedQuery().execute(Tuple.from(paramObj))
        .map(r -> resultMapper(r, isList))
        .onSuccess(fut::complete)
        .onFailure(f -> {
          f.printStackTrace();
          fut.fail(f);
        });
    return new CompletableFuture();
  }

  @Override
  public CompletableFuture runPagedQuery(ResolvedPagedPgQuery pgQuery,
      boolean isList) {
    Optional<Integer> limit = Optional.ofNullable(getEnvironment().getArgument("limit"));
    Optional<Integer> offset = Optional.ofNullable(getEnvironment().getArgument("offset"));
    Object[] paramObj = new Object[pgQuery.getQuery().getParameters().size()];
    for (int i = 0; i < pgQuery.getQuery().getParameters().size(); i++) {
      PgParameterHandler param = pgQuery.getQuery().getParameters().get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }

    //Add limit + offset
    final String query = String.format("SELECT * FROM (%s) x LIMIT %s OFFSET %s",
        pgQuery.getQuery().getSql(),
        limit.map(Object::toString).orElse("ALL"),
        offset.orElse(0)
    );

    log.info("Paged Query: " + query + " : " + Arrays.toString(paramObj));

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
  public Object visitSourcePgParameter(SourcePgParameter sourceParameter,
      QueryExecutionContext context) {
    return context.getContext().createPropertyFetcher(sourceParameter.getKey())
        .get(context.getEnvironment());
  }

  @Override
  public Object visitArgumentPgParameter(ArgumentPgParameter argumentParameter,
      QueryExecutionContext context) {
    return context.getArguments().stream()
        .filter(arg -> arg.getPath().equalsIgnoreCase(argumentParameter.getPath()))
        .findFirst()
        .map(f -> f.getValue())
        .orElse(null);
  }
}
