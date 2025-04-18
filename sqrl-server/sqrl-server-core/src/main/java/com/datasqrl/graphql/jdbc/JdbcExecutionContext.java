package com.datasqrl.graphql.jdbc;

import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import graphql.schema.DataFetchingEnvironment;
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
import lombok.SneakyThrows;
import lombok.Value;

@Value
public class JdbcExecutionContext extends AbstractQueryExecutionContext {

  JdbcContext context;
  DataFetchingEnvironment environment;
  Set<Argument> arguments;

  @SneakyThrows
  @Override
  public CompletableFuture runQuery(ResolvedSqlQuery resolvedQuery,
      boolean isList) {
    PreparedSqrlQueryImpl preparedQueryContainer = ((PreparedSqrlQueryImpl) resolvedQuery.getPreparedQueryContainer());
    final List paramObj = getParamArguments(resolvedQuery.getQuery().getParameters());
    SqlQuery query = resolvedQuery.getQuery();
    String unpreparedSqlQuery = query.getSql();
    switch (query.getPagination()) {
      case NONE: break;
      case LIMIT_AND_OFFSET:
        Optional<Integer> limit = Optional.ofNullable(getEnvironment().getArgument(LIMIT));
        Optional<Integer> offset = Optional.ofNullable(getEnvironment().getArgument(OFFSET));

        //special case where database doesn't support binding for limit/offset => need to execute dynamically
        if (!query.getDatabase().supportsLimitOffsetBinding) {
          assert preparedQueryContainer == null;
          unpreparedSqlQuery = JdbcExecutionContext.addLimitOffsetToQuery(unpreparedSqlQuery,
              limit.map(Object::toString).orElse("ALL"), String.valueOf(offset.orElse(0)));
        } else {
          paramObj.add(limit.orElse(Integer.MAX_VALUE));
          paramObj.add(offset.orElse(0));
        }
        break;
      default: throw new UnsupportedOperationException("Unsupported pagination: " + query.getPagination());
    }

    if (preparedQueryContainer!= null) {
      return CompletableFuture.supplyAsync(() -> {
        try (PreparedStatement statement = preparedQueryContainer.getConnection()
            .prepareStatement(preparedQueryContainer.getPreparedQuery())) {
          for (int i = 0; i < paramObj.size(); i++) {
            statement.setObject(i + 1, paramObj.get(i));
          }
          ResultSet resultSet = statement.executeQuery();

          return unboxList(resultSetToList(resultSet), isList);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    } else {
      final String sqlQuery = unpreparedSqlQuery;
      return CompletableFuture.supplyAsync(()-> {
        Connection connection = this.context.getClient().getConnection();

        try (PreparedStatement statement = connection.prepareStatement(sqlQuery)) {
          for (int i = 0; i < paramObj.size(); i++) {
            statement.setObject(i + 1, paramObj.get(i));
          }
          ResultSet resultSet = statement.executeQuery();

          return unboxList(resultSetToList(resultSet), isList);
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      });
    }
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


}
