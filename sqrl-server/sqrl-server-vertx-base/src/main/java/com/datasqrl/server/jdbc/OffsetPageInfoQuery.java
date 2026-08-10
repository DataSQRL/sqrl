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

import com.datasqrl.server.PaginationType;
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import io.vertx.core.json.JsonObject;
import java.util.List;

/**
 * A {@link PaginationType#OFFSET_PAGE_INFO} request: the rows of a page plus the {@code
 * OffsetPageInfo} metadata around them.
 *
 * <p>Everything here is decided up front from the GraphQL request and computed in memory - it runs
 * no queries. {@link VertxQueryExecutionContext} executes what {@link #pageQuery} and {@link
 * #aggregateSql} ask for and hands the rows back to {@link #toPage}:
 *
 * <ol>
 *   <li>{@link #from} reads the selection set once. Which metadata fields were selected is the only
 *       thing that decides what has to be executed.
 *   <li>{@link #pageQuery} is the same limit/offset query {@link PaginationType#LIMIT_AND_OFFSET}
 *       runs, fetching one extra row when {@code hasNextPage}/{@code nextOffset} were selected -
 *       that extra row is how they are answered without a COUNT.
 *   <li>{@link #aggregateSql} picks the one companion query, if any, that covers the remaining
 *       selected fields - never more than a single extra round trip.
 *   <li>{@link #toPage} trims that extra row back off and assembles {@code {results, pagination}}.
 * </ol>
 */
final class OffsetPageInfoQuery {

  private static final String FIRST_EVENT_TIME_COLUMN = "first_event_time";
  private static final String LAST_EVENT_TIME_COLUMN = "last_event_time";
  private static final String TOTAL_RECORDS_COLUMN = "total_records";

  private final PageRequest page;
  private final PageFields fields;
  private final boolean needsNextPage;
  private final boolean needsEventTimes;
  private final boolean needsTotals;

  private OffsetPageInfoQuery(
      PageRequest page,
      PageFields fields,
      boolean needsNextPage,
      boolean needsEventTimes,
      boolean needsTotals) {
    this.page = page;
    this.fields = fields;
    this.needsNextPage = needsNextPage;
    this.needsEventTimes = needsEventTimes;
    this.needsTotals = needsTotals;
  }

  static OffsetPageInfoQuery from(DataFetchingEnvironment environment) {
    var fields = pageFields(environment.getFieldType());
    var pagination = fields.pagination();
    var selection = environment.getSelectionSet();
    return new OffsetPageInfoQuery(
        PageRequest.from(environment),
        fields,
        selection.containsAnyOf(pagination + "/hasNextPage", pagination + "/nextOffset"),
        selection.containsAnyOf(pagination + "/firstEventTime", pagination + "/lastEventTime"),
        selection.containsAnyOf(pagination + "/totalRecords", pagination + "/totalPages"));
  }

  /**
   * The query returning the page rows, over-fetching by one row when a next page is in question.
   */
  PageRequest.BoundQuery pageQuery(SqlQuery query, List<Object> baseParams) {
    return (fetchesExtraRow() ? page.plusOneRow() : page).applyTo(query, baseParams);
  }

  /**
   * The single companion aggregate to run for this request, or null when the selected fields need
   * none. Selecting event times and totals together picks the query computing both, so the request
   * never costs more than one extra round trip - and never computes an aggregate nobody asked for.
   * Event times are only available when the query has a rowtime column.
   */
  String aggregateSql(SqlQuery query) {
    var eventTimes = needsEventTimes && query.getEventTimesSql() != null;
    if (needsTotals) {
      return eventTimes ? query.getCountWithEventTimesSql() : query.getCountSql();
    }
    return eventTimes ? query.getEventTimesSql() : null;
  }

  /**
   * Assembles the page from the rows the caller fetched. {@code aggregate} is the single row of the
   * companion aggregate, or an empty object when none ran.
   */
  JsonObject toPage(List<JsonObject> rows, JsonObject aggregate) {
    Boolean hasNextPage = null;
    if (needsNextPage) {
      hasNextPage = fetchesExtraRow() && rows.size() > page.limit();
      if (hasNextPage) {
        rows = rows.subList(0, page.limit());
      }
    }

    var pagination =
        paginationMetadata(
            page.pageSize(rows.size()),
            page.offset(),
            hasNextPage,
            aggregate.getValue(FIRST_EVENT_TIME_COLUMN),
            aggregate.getValue(LAST_EVENT_TIME_COLUMN),
            aggregate.getLong(TOTAL_RECORDS_COLUMN));

    return new JsonObject().put(fields.results(), rows).put(fields.pagination(), pagination);
  }

  /**
   * {@code hasNextPage}/{@code nextOffset} are derived from one over-fetched row. An unbounded page
   * holds every remaining row, so there is no next page and nothing to over-fetch.
   */
  private boolean fetchesExtraRow() {
    return needsNextPage && !page.unbounded();
  }

  /**
   * Builds the {@code OffsetPageInfo} object. A null {@code hasNextPage} or {@code totalRecords}
   * means the request did not select the fields deriving from it, so they are left out entirely -
   * GraphQL never reads them.
   */
  static JsonObject paginationMetadata(
      int pageSize,
      int offset,
      Boolean hasNextPage,
      Object firstEventTime,
      Object lastEventTime,
      Long totalRecords) {
    var hasPreviousPage = offset > 0;
    var pagination =
        new JsonObject()
            .put("pageSize", pageSize)
            .put("currentPage", pageSize == 0 ? 1 : offset / pageSize + 1)
            .put("hasPreviousPage", hasPreviousPage)
            .put(
                "prevOffset",
                hasPreviousPage ? Integer.valueOf(Math.max(0, offset - pageSize)) : null)
            .put("firstEventTime", firstEventTime)
            .put("lastEventTime", lastEventTime);

    if (hasNextPage != null) {
      pagination
          .put("hasNextPage", hasNextPage)
          .put("nextOffset", hasNextPage ? Integer.valueOf(offset + pageSize) : null);
    }
    if (totalRecords != null) {
      pagination
          .put("totalRecords", totalRecords)
          .put("totalPages", pageSize == 0 ? 0 : (int) Math.ceil((double) totalRecords / pageSize));
    }
    return pagination;
  }

  /** The results/pagination field names of the page wrapper type this request returns. */
  private static PageFields pageFields(GraphQLOutputType fieldType) {
    var objectType = (GraphQLObjectType) unwrapNonNull(fieldType);
    String results = null;
    String pagination = null;
    for (var field : objectType.getFieldDefinitions()) {
      if (unwrapNonNull(field.getType()) instanceof GraphQLList) {
        results = field.getName();
      } else {
        pagination = field.getName();
      }
    }
    return new PageFields(results, pagination);
  }

  private static GraphQLOutputType unwrapNonNull(GraphQLOutputType type) {
    return type instanceof GraphQLNonNull nonNull
        ? (GraphQLOutputType) nonNull.getWrappedType()
        : type;
  }

  private record PageFields(String results, String pagination) {}
}
