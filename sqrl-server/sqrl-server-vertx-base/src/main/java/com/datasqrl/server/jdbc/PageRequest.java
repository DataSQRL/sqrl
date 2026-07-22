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
import com.datasqrl.server.graphql.RootGraphQLModel.SqlQuery;
import graphql.schema.DataFetchingEnvironment;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * The {@code limit}/{@code offset} arguments of one request. A null limit means the caller did not
 * ask for one, so the query returns every remaining row.
 *
 * <p>This is the only piece {@link PaginationType#LIMIT_AND_OFFSET} and {@link
 * PaginationType#OFFSET_PAGE_INFO} share: both slice the result the same way, they differ only in
 * what they return around the rows.
 */
record PageRequest(Integer limit, int offset) {

  /** Bind parameters must be a number; this stands in for "no limit" when binding. */
  private static final int NO_LIMIT = Integer.MAX_VALUE;

  static PageRequest from(DataFetchingEnvironment environment) {
    Integer limit = environment.getArgument(LIMIT);
    int offset = Optional.<Integer>ofNullable(environment.getArgument(OFFSET)).orElse(0);
    return new PageRequest(limit, offset);
  }

  boolean unbounded() {
    return limit == null;
  }

  /** The number of rows on the page, which for an unbounded request is whatever came back. */
  int pageSize(int rowCount) {
    return unbounded() ? rowCount : limit;
  }

  /** Same page, one row longer. An unbounded page already holds every row, so it is unchanged. */
  PageRequest plusOneRow() {
    return unbounded() ? this : new PageRequest(limit + 1, offset);
  }

  /**
   * Applies this page to {@code query}. Databases that support binding get limit/offset appended as
   * bind parameters, the others (e.g. Snowflake) get them written into the SQL text. {@code
   * baseParams} is never modified: the caller may still need it for a companion query.
   */
  BoundQuery applyTo(SqlQuery query, List<Object> baseParams) {
    if (!query.getDatabase().supportsLimitOffsetBinding) {
      var sql =
          AbstractQueryExecutionContext.addLimitOffsetToQuery(
              query.getSql(), unbounded() ? "ALL" : String.valueOf(limit), String.valueOf(offset));
      return new BoundQuery(sql, baseParams);
    }

    // ArrayList rather than List.copyOf: parameter values may be null
    var params = new ArrayList<>(baseParams);
    params.add(unbounded() ? NO_LIMIT : limit);
    params.add(offset);
    return new BoundQuery(query.getSql(), params);
  }

  /** The SQL to execute and the full parameter list to bind to it. */
  record BoundQuery(String sql, List<Object> params) {}
}
