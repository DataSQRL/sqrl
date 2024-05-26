package com.datasqrl.graphql;

import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.graphql.VertxJdbcClient.PreparedSqrlQueryImpl;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentParameter;
import com.datasqrl.graphql.server.RootGraphqlModel.ParameterHandlerVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcParameterHandler;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedCalciteQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedPagedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SourceParameter;
import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.GraphQLEngineBuilder;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
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

  @Override
  public Future runQuery(GraphQLEngineBuilder server, ResolvedJdbcQuery pgQuery,
      boolean isList) {
    Object[] paramObj = new Object[pgQuery.getQuery().getParameters().size()];
    for (int i = 0; i < pgQuery.getQuery().getParameters().size(); i++) {
      JdbcParameterHandler param = pgQuery.getQuery().getParameters().get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }

    return ((PreparedSqrlQueryImpl) pgQuery.getPreparedQueryContainer())
        .getPreparedQuery().execute(Tuple.from(paramObj))
        .map(r -> resultMapper(r, isList));
  }

  @Override
  public Future runPagedJdbcQuery(ResolvedPagedJdbcQuery pgQuery,
      boolean isList, QueryExecutionContext context) {
    Optional<Integer> limit = Optional.ofNullable(getEnvironment().getArgument(LIMIT));
    Optional<Integer> offset = Optional.ofNullable(getEnvironment().getArgument(OFFSET));
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

    return this.context.getSqlClient()
        .getSqlClient()
        .preparedQuery(query)
        .execute(Tuple.from(paramObj))
        .map(r -> resultMapper(r, isList));
  }

  @SneakyThrows
  @Override
  public Future runCalciteQuery(ResolvedCalciteQuery query, boolean list,
      QueryExecutionContext context) {

    Promise promise = Promise.promise();

    try {
      // Create a statement from the Calcite connection
      Statement statement = this.context.getCalciteClient().createStatement();

        // Execute the query specified in the ResolvedCalciteQuery
      ResultSet resultSet = statement.executeQuery(query.getQuery().getSql());

      // Process the ResultSet into a JSON structure
      List<JsonObject> results = new ArrayList<>();
      while (resultSet.next()) {
        JsonObject jsonObject = new JsonObject();
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();
        for (int i = 1; i <= columnCount; i++) {
          String columnName = metaData.getColumnName(i);
          jsonObject.put(columnName, resultSet.getObject(i));
        }
        results.add(jsonObject);
      }

      // Decide on the return structure based on isList flag
      if (list) {
        promise.complete(results);
      } else {
        if (results.isEmpty()) {
          promise.complete(new JsonObject());
        } else {
          promise.complete(results.get(0));
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
      promise.fail(e);
    }

    return promise.future();
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
        .filter(arg -> ((Argument)arg).getPath().equalsIgnoreCase(argumentParameter.getPath()))
        .findFirst()
        .map(f -> ((Argument)f).getValue())
        .orElse(null);
  }
}
