/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.server.jdbc;

import static com.datasqrl.server.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.server.jdbc.SchemaConstants.OFFSET;

import com.datasqrl.server.VertxServerContext;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.Argument;
import com.datasqrl.server.graphql.RootGraphQLModel.ResolvedSqlQuery;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

/**
 * It is the ExecutionContext per servlet type. It is responsible for executing the resolved SQL
 * queries (paginated or not) in Vert.x and mapping the database resultSet to json for using in
 * GraphQL responses. It also implements the parameters and arguments visitors for the {@link
 * RootGraphQLModel} visitors
 */
public class VertxQueryExecutionContext extends AbstractQueryExecutionContext<VertxServerContext> {

  final CompletableFuture<Object> cf;

  public VertxQueryExecutionContext(
      VertxServerContext serverCtx,
      DataFetchingEnvironment environment,
      Set<Argument> arguments,
      CompletableFuture<Object> cf,
      ParamArgumentTypeMapper paramArgumentTypeMapper) {
    super(serverCtx, environment, arguments, paramArgumentTypeMapper);
    this.cf = cf;
  }

  @Override
  public CompletableFuture<Object> runQuery(ResolvedSqlQuery resolvedQuery, boolean isList) {
    getParamArgumentsFuture(resolvedQuery.getQuery().getParameters())
        .whenComplete(
            (paramObj, throwable) -> {
              if (throwable != null) {
                cf.completeExceptionally(throwable);
              } else {
                runQueryInternal(resolvedQuery, isList, paramObj);
              }
            });

    return cf;
  }

  private CompletableFuture<Object> runQueryInternal(
      ResolvedSqlQuery resolvedQuery, boolean isList, List<Object> paramObj) {

    var preparedQueryContainer = (PreparedVertxSqrlQuery) resolvedQuery.getPreparedQueryContainer();
    var countContainer = (PreparedVertxSqrlQuery) resolvedQuery.getPreparedCountQueryContainer();
    var query = resolvedQuery.getQuery();
    var unpreparedSqlQuery = query.getSql();
    var database = query.getDatabase();
    var paged = query.getCountSql() != null;

    // The count query is bound with the base parameters only, without the runtime limit/offset.
    var countParams = paged ? Tuple.from(List.copyOf(paramObj)) : null;

    int limitValue = Integer.MAX_VALUE;
    int offsetValue = 0;
    switch (query.getPagination()) {
      case NONE:
        break;
      case LIMIT_AND_OFFSET:
        var limit = Optional.<Integer>ofNullable(environment.getArgument(LIMIT));
        var offset = Optional.<Integer>ofNullable(environment.getArgument(OFFSET));
        limitValue = limit.orElse(Integer.MAX_VALUE);
        offsetValue = offset.orElse(0);

        // special case where database doesn't support binding for limit/offset => need
        // to execute dynamically
        if (!query.getDatabase().supportsLimitOffsetBinding) {
          assert preparedQueryContainer == null;
          unpreparedSqlQuery =
              AbstractQueryExecutionContext.addLimitOffsetToQuery(
                  unpreparedSqlQuery,
                  limit.map(Object::toString).orElse("ALL"),
                  String.valueOf(offset.orElse(0)));
        } else {
          paramObj.add(limit.orElse(Integer.MAX_VALUE));
          paramObj.add(offset.orElse(0));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported pagination: " + query.getPagination());
    }

    // execute the preparedQuery with the arguments extracted above
    Future<RowSet<Row>> future;
    var params = Tuple.from(paramObj);

    if (preparedQueryContainer == null) {
      future = serverContext.getSqlClient().execute(database, unpreparedSqlQuery, params);
    } else {
      var preparedQuery = preparedQueryContainer.preparedQuery();
      future = serverContext.getSqlClient().execute(preparedQuery, params);
    }

    if (paged) {
      Future<RowSet<Row>> countFuture =
          countContainer == null
              ? serverContext.getSqlClient().execute(database, query.getCountSql(), countParams)
              : serverContext.getSqlClient().execute(countContainer.preparedQuery(), countParams);
      var dataFuture = future;
      var effectiveLimit = limitValue;
      var effectiveOffset = offsetValue;
      Future.all(dataFuture, countFuture)
          .map(
              c ->
                  pagedResultMapper(
                      dataFuture.result(), countFuture.result(), effectiveLimit, effectiveOffset))
          .onSuccess(cf::complete)
          .onFailure(
              f -> {
                f.printStackTrace();
                cf.completeExceptionally(f);
              });
      return cf;
    }

    // map the resultSet to json for GraphQL response
    future
        .map(r -> resultMapper(r, isList))
        .onSuccess(cf::complete)
        .onFailure(
            f -> {
              f.printStackTrace();
              cf.completeExceptionally(f);
            });
    return cf;
  }

  private Object resultMapper(RowSet<Row> r, boolean isList) {
    var o = StreamSupport.stream(r.spliterator(), false).map(Row::toJson).toList();

    return unboxList(o, isList);
  }

  private Object pagedResultMapper(
      RowSet<Row> dataRows, RowSet<Row> countRows, int limit, int offset) {
    var results = StreamSupport.stream(dataRows.spliterator(), false).map(Row::toJson).toList();

    var countIterator = countRows.iterator();
    var countJson = countIterator.hasNext() ? countIterator.next().toJson() : new JsonObject();
    var totalRecords = countJson.getLong("total_records", 0L);
    var pagination =
        buildPaginationMetadata(
            totalRecords,
            limit,
            offset,
            countJson.getValue("first_event_time"),
            countJson.getValue("last_event_time"));

    var fieldNames = pageFieldNames(environment.getFieldType());
    return new JsonObject()
        .put(fieldNames.resultsField(), results)
        .put(fieldNames.paginationField(), pagination);
  }

  /** Derives the results/pagination field names from the page wrapper's GraphQL object type. */
  private static PageFieldNames pageFieldNames(GraphQLOutputType fieldType) {
    if (fieldType instanceof GraphQLNonNull g) {
      fieldType = (GraphQLOutputType) g.getWrappedType();
    }
    var objectType = (GraphQLObjectType) fieldType;
    String resultsField = null;
    String paginationField = null;
    for (var field : objectType.getFieldDefinitions()) {
      var type = field.getType();
      if (type instanceof GraphQLNonNull g) {
        type = (GraphQLOutputType) g.getWrappedType();
      }
      if (type instanceof GraphQLList) {
        resultsField = field.getName();
      } else {
        paginationField = field.getName();
      }
    }
    return new PageFieldNames(resultsField, paginationField);
  }

  private record PageFieldNames(String resultsField, String paginationField) {}

  static JsonObject buildPaginationMetadata(
      long totalRecords, int limit, int offset, Object firstEventTime, Object lastEventTime) {
    int totalPages = limit == 0 ? 0 : (int) Math.ceil((double) totalRecords / limit);
    int currentPage = limit == 0 ? 1 : offset / limit + 1;
    boolean hasNextPage = (long) offset + limit < totalRecords;
    boolean hasPreviousPage = offset > 0;

    return new JsonObject()
        .put("totalRecords", totalRecords)
        .put("pageSize", limit)
        .put("currentPage", currentPage)
        .put("totalPages", totalPages)
        .put("hasNextPage", hasNextPage)
        .put("hasPreviousPage", hasPreviousPage)
        .put("nextOffset", hasNextPage ? Integer.valueOf(offset + limit) : null)
        .put("prevOffset", hasPreviousPage ? Integer.valueOf(Math.max(0, offset - limit)) : null)
        .put("firstEventTime", firstEventTime)
        .put("lastEventTime", lastEventTime);
  }
}
