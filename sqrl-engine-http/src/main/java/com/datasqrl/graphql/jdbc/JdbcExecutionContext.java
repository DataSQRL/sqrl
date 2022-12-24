package com.datasqrl.graphql.jdbc;

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
import graphql.schema.PropertyDataFetcher;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Value;
import org.jooq.impl.DSL;

@Value
public class JdbcExecutionContext implements QueryExecutionContext,
    ParameterHandlerVisitor<Object, QueryExecutionContext> {

  JdbcContext context;
  DataFetchingEnvironment environment;
  Set<FixedArgument> arguments;

  @Override
  public CompletableFuture runQuery(SqrlGraphQLServer sqrlGraphQLServer, ResolvedPgQuery pgQuery,
      boolean isList) {
    Object[] paramObj = new Object[pgQuery.getQuery().getParameters().size()];
    for (int i = 0; i < pgQuery.getQuery().getParameters().size(); i++) {
      PgParameterHandler param = pgQuery.getQuery().getParameters().get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }

    //Look at graphql response for list type here
    PreparedSqrlQueryImpl p = ((PreparedSqrlQueryImpl) pgQuery.getPreparedQueryContainer());

    try (PreparedStatement statement = p.getConnection()
        .prepareStatement(p.getPreparedQuery())) {
      for (int i = 0; i < paramObj.length; i++) {
        statement.setObject(i + 1, paramObj[i]);
      }
      ResultSet resultSet = statement.executeQuery();
      return CompletableFuture.completedFuture(unbox(DSL.using(p.getConnection())
          .fetch(resultSet)
          .intoMaps(), isList));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public static Object unbox(List<Map<String, Object>> o, boolean isList) {

    return isList
        ? o
        : (o.size() > 0 ? o.get(0) : null);
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

    Connection connection = context.getClient().getConnection();
    try (PreparedStatement statement = connection.prepareStatement(query)) {
      for (int i = 0; i < paramObj.length; i++) {
        statement.setObject(i + 1, paramObj[i]);
      }
      ResultSet resultSet = statement.executeQuery();
      return CompletableFuture.completedFuture(unbox(DSL.using(connection)
          .fetch(resultSet)
          .intoMaps(), isList));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object visitSourcePgParameter(SourcePgParameter sourceParameter,
      QueryExecutionContext context) {
    return PropertyDataFetcher.fetching(sourceParameter.getKey())
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
