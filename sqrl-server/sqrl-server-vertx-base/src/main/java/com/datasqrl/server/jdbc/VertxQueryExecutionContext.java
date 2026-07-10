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

import com.datasqrl.server.PaginationType;
import com.datasqrl.server.VertxServerContext;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.Argument;
import com.datasqrl.server.graphql.RootGraphQLModel.ResolvedSqlQuery;
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
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
              } else if (resolvedQuery.getQuery().getPagination()
                  == PaginationType.OFFSET_PAGE_INFO) {
                runPaginatedQuery(resolvedQuery, paramObj);
              } else {
                runPlainQuery(resolvedQuery, isList, paramObj);
              }
            });

    return cf;
  }

  /** Executes a bare (non-paged) query, applying limit/offset when the query declares them. */
  private void runPlainQuery(
      ResolvedSqlQuery resolvedQuery, boolean isList, List<Object> paramObj) {
    var query = resolvedQuery.getQuery();
    var sql = query.getSql();
    if (query.getPagination() == PaginationType.LIMIT_AND_OFFSET) {
      var limit = Optional.<Integer>ofNullable(environment.getArgument(LIMIT));
      var offset = Optional.<Integer>ofNullable(environment.getArgument(OFFSET));
      sql =
          applyLimitOffset(
              query,
              sql,
              paramObj,
              limit.orElse(Integer.MAX_VALUE),
              offset.orElse(0),
              limit.isPresent());
    }

    execute(resolvedQuery, sql, paramObj)
        .map(r -> resultMapper(r, isList))
        .onSuccess(cf::complete)
        .onFailure(
            f -> {
              f.printStackTrace();
              cf.completeExceptionally(f);
            });
  }

  /**
   * Executes an {@link PaginationType#OFFSET_PAGE_INFO} query: the page data query always runs,
   * while the pagination metadata is computed lazily from the selection set. The event-time
   * aggregate runs only when an event-time field is selected; {@code hasNextPage}/{@code
   * nextOffset} are answered by fetching one extra row instead of any aggregate.
   */
  private void runPaginatedQuery(ResolvedSqlQuery resolvedQuery, List<Object> paramObj) {
    var query = resolvedQuery.getQuery();
    var fieldNames = pageFieldNames(environment.getFieldType());
    var pag = fieldNames.paginationField();
    var selection = environment.getSelectionSet();

    var needEventTimes = selection.containsAnyOf(pag + "/firstEventTime", pag + "/lastEventTime");
    var eventTimesSql = needEventTimes ? query.getEventTimesSql() : null;

    var limit = Optional.<Integer>ofNullable(environment.getArgument(LIMIT));
    var offset = Optional.<Integer>ofNullable(environment.getArgument(OFFSET));
    var limitValue = limit.orElse(Integer.MAX_VALUE);
    var offsetValue = offset.orElse(0);

    // hasNextPage/nextOffset are derived by fetching one extra row. Without a limit the page holds
    // every remaining row, so there is no next page and no extra row to fetch.
    var deriveNext = selection.containsAnyOf(pag + "/hasNextPage", pag + "/nextOffset");
    var fetchExtraRow = deriveNext && limitValue != Integer.MAX_VALUE;
    var fetchLimit = fetchExtraRow ? limitValue + 1 : limitValue;

    // The aggregate binds the base parameters only. Tuple.from snapshots the list here (before
    // limit/offset are appended below) and, unlike List.copyOf, tolerates null bind values.
    var aggregateParams = eventTimesSql != null ? Tuple.from(paramObj) : null;

    var sql =
        applyLimitOffset(
            query, query.getSql(), paramObj, fetchLimit, offsetValue, limit.isPresent());

    var dataFuture = execute(resolvedQuery, sql, paramObj);
    Future<RowSet<Row>> aggregateFuture =
        eventTimesSql == null
            ? Future.succeededFuture(null)
            : serverContext
                .getSqlClient()
                .execute(query.getDatabase(), eventTimesSql, aggregateParams);

    Future.all(dataFuture, aggregateFuture)
        .map(
            c ->
                pagedResultMapper(
                    dataFuture.result(),
                    aggregateFuture.result(),
                    fieldNames,
                    limitValue,
                    offsetValue,
                    fetchExtraRow,
                    deriveNext))
        .onSuccess(cf::complete)
        .onFailure(
            f -> {
              f.printStackTrace();
              cf.completeExceptionally(f);
            });
  }

  /**
   * Binds limit/offset as parameters (databases that support it) or rewrites the SQL text
   * (databases that don't, e.g. Snowflake). Returns the SQL to execute.
   */
  private static String applyLimitOffset(
      SqlQuery query,
      String sql,
      List<Object> paramObj,
      int limit,
      int offset,
      boolean limitPresent) {
    if (!query.getDatabase().supportsLimitOffsetBinding) {
      return AbstractQueryExecutionContext.addLimitOffsetToQuery(
          sql, limitPresent ? String.valueOf(limit) : "ALL", String.valueOf(offset));
    }
    paramObj.add(limit);
    paramObj.add(offset);
    return sql;
  }

  private Future<RowSet<Row>> execute(
      ResolvedSqlQuery resolvedQuery, String sql, List<Object> paramObj) {
    var container = (PreparedVertxSqrlQuery) resolvedQuery.getPreparedQueryContainer();
    var params = Tuple.from(paramObj);
    if (container == null) {
      return serverContext
          .getSqlClient()
          .execute(resolvedQuery.getQuery().getDatabase(), sql, params);
    }
    return serverContext.getSqlClient().execute(container.preparedQuery(), params);
  }

  private Object resultMapper(RowSet<Row> r, boolean isList) {
    var o = StreamSupport.stream(r.spliterator(), false).map(Row::toJson).toList();

    return unboxList(o, isList);
  }

  private Object pagedResultMapper(
      RowSet<Row> dataRows,
      RowSet<Row> eventTimeRows,
      PageFieldNames fieldNames,
      int limit,
      int offset,
      boolean extraRowFetched,
      boolean deriveNextFromResults) {
    var results = StreamSupport.stream(dataRows.spliterator(), false).map(Row::toJson).toList();

    Boolean hasNextPage = null;
    if (extraRowFetched) {
      hasNextPage = results.size() > limit;
      if (hasNextPage) {
        results = results.subList(0, limit);
      }
    } else if (deriveNextFromResults) {
      hasNextPage = false; // no limit given: the page contains every remaining row
    }

    Object firstEventTime = null;
    Object lastEventTime = null;
    if (eventTimeRows != null) {
      var it = eventTimeRows.iterator();
      var eventTimeJson = it.hasNext() ? it.next().toJson() : new JsonObject();
      firstEventTime = eventTimeJson.getValue("first_event_time");
      lastEventTime = eventTimeJson.getValue("last_event_time");
    }

    var pagination =
        buildPaginationMetadata(hasNextPage, limit, offset, firstEventTime, lastEventTime);
    // An absent limit means "return everything" (limit == Integer.MAX_VALUE); report the actual
    // number of rows on this page as pageSize rather than leaking the sentinel.
    if (limit == Integer.MAX_VALUE) {
      pagination.put("pageSize", results.size());
    }
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

  /**
   * Builds the pagination metadata object. {@code hasNextPage} is null when the request did not
   * select fields requiring it; the corresponding fields are then left out (GraphQL never reads
   * unselected fields).
   */
  static JsonObject buildPaginationMetadata(
      Boolean hasNextPage, int limit, int offset, Object firstEventTime, Object lastEventTime) {
    boolean hasPreviousPage = offset > 0;
    var pagination =
        new JsonObject()
            .put("pageSize", limit)
            .put("currentPage", limit == 0 ? 1 : offset / limit + 1)
            .put("hasPreviousPage", hasPreviousPage)
            .put(
                "prevOffset", hasPreviousPage ? Integer.valueOf(Math.max(0, offset - limit)) : null)
            .put("firstEventTime", firstEventTime)
            .put("lastEventTime", lastEventTime);

    if (hasNextPage != null) {
      pagination
          .put("hasNextPage", hasNextPage)
          .put("nextOffset", hasNextPage ? Integer.valueOf(offset + limit) : null);
    }
    return pagination;
  }
}
