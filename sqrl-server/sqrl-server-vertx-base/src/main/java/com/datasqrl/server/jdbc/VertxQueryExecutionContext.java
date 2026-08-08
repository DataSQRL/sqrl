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

import com.datasqrl.server.VertxServerContext;
import com.datasqrl.server.graphql.RootGraphQLModel;
import com.datasqrl.server.graphql.RootGraphQLModel.Argument;
import com.datasqrl.server.graphql.RootGraphQLModel.ResolvedSqlQuery;
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
import com.datasqrl.server.jdbc.PageRequest.BoundQuery;
import graphql.schema.DataFetchingEnvironment;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.StreamSupport;

/**
 * It is the ExecutionContext per servlet type. It is responsible for executing the resolved SQL
 * queries (paginated or not) in Vert.x and mapping the database resultSet to json for using in
 * GraphQL responses. It also implements the parameters and arguments visitors for the {@link
 * RootGraphQLModel} visitors
 *
 * <p>Each pagination type has its own execution path; the pagination logic itself lives in {@link
 * PageRequest} and {@link OffsetPageInfoQuery}, this class only runs queries and maps rows.
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
    var query = resolvedQuery.getQuery();
    getParamArgumentsFuture(query.getParameters())
        .whenComplete(
            (params, throwable) -> {
              if (throwable != null) {
                cf.completeExceptionally(throwable);
                return;
              }
              switch (query.getPagination()) {
                case NONE ->
                    runRowQuery(resolvedQuery, new BoundQuery(query.getSql(), params), isList);
                case LIMIT_AND_OFFSET ->
                    runRowQuery(
                        resolvedQuery,
                        PageRequest.from(environment).applyTo(query, params),
                        isList);
                case OFFSET_PAGE_INFO -> runOffsetPageInfoQuery(resolvedQuery, params);
                default ->
                    cf.completeExceptionally(
                        new UnsupportedOperationException(
                            "Unsupported pagination: " + query.getPagination()));
              }
            });

    return cf;
  }

  /** Completes with the rows themselves: no page wrapper, no metadata. */
  private void runRowQuery(ResolvedSqlQuery resolvedQuery, BoundQuery boundQuery, boolean isList) {
    execute(resolvedQuery, boundQuery)
        .map(rows -> unboxList(toJson(rows), isList))
        .onSuccess(cf::complete)
        .onFailure(this::failQuery);
  }

  /**
   * Completes with a page: the page query always runs, the companion aggregates only when the
   * request selected fields needing them - the rowtime MIN/MAX behind {@code firstEventTime}/{@code
   * lastEventTime} (and only if the query has a rowtime column), and the COUNT behind {@code
   * totalRecords}/{@code totalPages}. {@link OffsetPageInfoQuery} decides all of it and assembles
   * the response.
   */
  private void runOffsetPageInfoQuery(ResolvedSqlQuery resolvedQuery, List<Object> params) {
    var query = resolvedQuery.getQuery();
    var pageInfoQuery = OffsetPageInfoQuery.from(environment);

    var pageFuture = execute(resolvedQuery, pageInfoQuery.pageQuery(query, params));
    var eventTimesFuture =
        pageInfoQuery.needsEventTimes() && query.getEventTimesSql() != null
            ? executeAggregate(query, query.getEventTimesSql(), params)
            : Future.<RowSet<Row>>succeededFuture(null);
    var totalsFuture =
        pageInfoQuery.needsTotals() && query.getCountSql() != null
            ? executeAggregate(query, query.getCountSql(), params)
            : Future.<RowSet<Row>>succeededFuture(null);

    Future.all(pageFuture, eventTimesFuture, totalsFuture)
        .map(
            ignored ->
                pageInfoQuery.toPage(
                    toJson(pageFuture.result()),
                    firstRowAsJson(eventTimesFuture.result()),
                    firstRowAsJson(totalsFuture.result())))
        .onSuccess(cf::complete)
        .onFailure(this::failQuery);
  }

  private Future<RowSet<Row>> execute(ResolvedSqlQuery resolvedQuery, BoundQuery boundQuery) {
    var container = (PreparedVertxSqrlQuery) resolvedQuery.getPreparedQueryContainer();
    var params = Tuple.from(boundQuery.params());
    if (container == null) {
      return serverContext
          .getSqlClient()
          .execute(resolvedQuery.getQuery().getDatabase(), boundQuery.sql(), params);
    }
    return serverContext.getSqlClient().execute(container.preparedQuery(), params);
  }

  /** Companion aggregates are never prepared and bind the base parameters only. */
  private Future<RowSet<Row>> executeAggregate(SqlQuery query, String sql, List<Object> params) {
    return serverContext.getSqlClient().execute(query.getDatabase(), sql, Tuple.from(params));
  }

  private void failQuery(Throwable throwable) {
    throwable.printStackTrace();
    cf.completeExceptionally(throwable);
  }

  private static List<JsonObject> toJson(RowSet<Row> rows) {
    return StreamSupport.stream(rows.spliterator(), false).map(Row::toJson).toList();
  }

  /** The single row of an aggregate query, empty when the query did not run or returned nothing. */
  private static JsonObject firstRowAsJson(RowSet<Row> rows) {
    if (rows == null) {
      return new JsonObject();
    }
    var iterator = rows.iterator();
    return iterator.hasNext() ? iterator.next().toJson() : new JsonObject();
  }
}
