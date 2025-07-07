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
package com.datasqrl.graphql.query;

import com.datasqrl.graphql.server.QueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.server.query.ResolvedQuery.Preprocessor;
import com.datasqrl.graphql.server.query.ResolvedSqlQuery;
import com.datasqrl.graphql.server.query.SqlQueryModifier.UserSqlQuery;
import com.datasqrl.graphql.server.query.SqlQueryModifier.UserSqlQuery.Type;
import com.google.common.base.Preconditions;
import java.util.Locale;
import java.util.Optional;
import lombok.Value;

/**
 * This is a placeholder implementation
 *
 * <p>The "real" implementation (once we depend on Flink/Calcite) should: - support TRANSFORM as
 * well - register the table create statement in the catalog - parse the user query against the
 * catalog - analyze the query to validate it is proper (no sub-queries, only references to the base
 * table, etc) - then do the string replacement
 */
@Value
public class UserSqlQueryPreprocessor implements Preprocessor {

  UserSqlQuery userSqlQuery;

  public UserSqlQueryPreprocessor(UserSqlQuery userSqlQuery) {
    Preconditions.checkArgument(
        userSqlQuery.type() == Type.FILTER, "Currently, only filter queries are supported");
    this.userSqlQuery = userSqlQuery;
  }

  @Override
  public ResolvedSqlQuery preprocess(
      ResolvedSqlQuery resolvedSqlQuery, QueryExecutionContext context) {
    String whereClause = context.getEnvironment().getArgument(userSqlQuery.parameterName());
    if (whereClause == null || whereClause.isBlank()) return resolvedSqlQuery;
    SqlQuery oldQuery = resolvedSqlQuery.getQuery();
    String newQueryString = buildSafeQuery(oldQuery.getSql(), whereClause);
    SqlQuery newQuery = oldQuery.updateSQL(newQueryString);
    return new ResolvedSqlQuery(newQuery, null, Optional.empty());
  }

  /**
   * Build SELECT * FROM <tableName> WHERE <whereClause>
   *
   * <p>Basic rules: • tableName – single identifier, letters/digits/underscore only • whereClause–
   * no sub-queries, comments, or statement terminators
   */
  public static String buildSafeQuery(String baseQuery, String whereClause) {
    String wc = whereClause.trim();

    // ── cheap surface‐filters (no comments / multi statements) ─────────────────
    if (wc.contains(";") || wc.contains("--") || wc.contains("/*") || wc.contains("*/")) {
      throw new IllegalArgumentException("Comments or multiple statements are not allowed");
    }

    // ── sub-query guard (look for SELECT keyword outside string literals) ──────
    if (containsKeywordOutsideQuotes(wc, "SELECT")) {
      throw new IllegalArgumentException("Sub-queries are not permitted");
    }

    // ── parentheses & quotes sanity checks ─────────────────────────────────────
    if (!balancedParentheses(wc)) {
      throw new IllegalArgumentException("Unbalanced parentheses in WHERE clause");
    }
    if (!balancedSingleQuotes(wc)) {
      throw new IllegalArgumentException("Unmatched single quote in WHERE clause");
    }

    return "SELECT * FROM (" + baseQuery + ") WHERE " + wc;
  }

  // ───────────────────────── helpers ───────────────────────────────────────────

  /** true if keyword appears outside string literals (single quotes) */
  private static boolean containsKeywordOutsideQuotes(String s, String keyword) {
    String upper = s.toUpperCase(Locale.ROOT);
    keyword = keyword.toUpperCase(Locale.ROOT);
    boolean inQuote = false;
    for (int i = 0; i < upper.length(); i++) {
      char c = upper.charAt(i);
      if (c == '\'') inQuote = !inQuote;
      if (!inQuote && upper.startsWith(keyword, i)) {
        // make sure it's a word boundary
        int end = i + keyword.length();
        boolean boundaryStart = i == 0 || !Character.isLetterOrDigit(upper.charAt(i - 1));
        boolean boundaryEnd =
            end >= upper.length() || !Character.isLetterOrDigit(upper.charAt(end));
        if (boundaryStart && boundaryEnd) return true;
      }
    }
    return false;
  }

  private static boolean balancedParentheses(String s) {
    int depth = 0;
    boolean inQuote = false;
    for (char c : s.toCharArray()) {
      if (c == '\'') inQuote = !inQuote;
      if (inQuote) continue;
      if (c == '(') depth++;
      if (c == ')') {
        if (--depth < 0) return false;
      }
    }
    return depth == 0;
  }

  private static boolean balancedSingleQuotes(String s) {
    long count = s.chars().filter(ch -> ch == '\'').count();
    return count % 2 == 0; // crude but effective
  }
}
