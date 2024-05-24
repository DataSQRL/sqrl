package com.datasqrl.graphql.jdbc;

import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

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
import graphql.schema.PropertyDataFetcher;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import lombok.Value;

@Value
public class JdbcExecutionContext implements QueryExecutionContext,
    ParameterHandlerVisitor<Object, QueryExecutionContext> {

  JdbcContext context;
  DataFetchingEnvironment environment;
  Set<Argument> arguments;

  @Override
  public CompletableFuture runQuery(GraphQLEngineBuilder graphQLEngineBuilder, ResolvedJdbcQuery pgQuery,
      boolean isList) {

    Object[] paramObj = new Object[pgQuery.getQuery().getParameters().size()];
    for (int i = 0; i < pgQuery.getQuery().getParameters().size(); i++) {
      JdbcParameterHandler param = pgQuery.getQuery().getParameters().get(i);
      Object o = param.accept(this, this);
      paramObj[i] = o;
    }
    //Look at graphql response for list type here
    PreparedSqrlQueryImpl p = ((PreparedSqrlQueryImpl) pgQuery.getPreparedQueryContainer());

    return CompletableFuture.supplyAsync(()-> {
      try (PreparedStatement statement = p.getConnection()
          .prepareStatement(p.getPreparedQuery())) {
        for (int i = 0; i < paramObj.length; i++) {
          statement.setObject(i + 1, paramObj[i]);
        }
        ResultSet resultSet = statement.executeQuery();

        return unboxList(resultSetToList(resultSet), isList);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  public static Object unboxList(List<Map<String, Object>> o, boolean isList) {
    return isList
        ? o
        : (o.size() > 0 ? o.get(0) : null);
  }

  @Override
  public CompletableFuture runPagedJdbcQuery(ResolvedPagedJdbcQuery pgQuery,
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

    return CompletableFuture.supplyAsync(()-> {
      Connection connection = this.context.getClient().getConnection();

      try (PreparedStatement statement = connection.prepareStatement(query)) {
        for (int i = 0; i < paramObj.length; i++) {
          statement.setObject(i + 1, paramObj[i]);
        }
        ResultSet resultSet = statement.executeQuery();

        return unboxList(resultSetToList(resultSet), isList);
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    });
  }

  private List<Map<String, Object>> resultSetToList(ResultSet resultSet) throws SQLException {
    List<Map<String, Object>> rows = new ArrayList<>();
    ResultSetMetaData metaData = resultSet.getMetaData();
    int columnCount = metaData.getColumnCount();

    while (resultSet.next()) {
      Map<String, Object> row = new LinkedHashMap<>(columnCount);
      for (int i = 1; i <= columnCount; i++) {
        row.put(metaData.getColumnLabel(i), resultSet.getObject(i));
      }
      rows.add(row);
    }
    return rows;
  }

  @Override
  public Object visitSourceParameter(SourceParameter sourceParameter,
      QueryExecutionContext context) {
    return PropertyDataFetcher.fetching(sourceParameter.getKey())
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
