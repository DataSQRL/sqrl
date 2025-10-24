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
package com.datasqrl.calcite.dialect;

import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.validate.SqlConformance;

public class PostgresConformance implements SqlConformance {

  @Override
  public boolean isLiberal() {
    return false; // PostgreSQL adheres to standard SQL rules and is not considered 'liberal' in SQL
    // conformance.
  }

  @Override
  public boolean allowCharLiteralAlias() {
    return true; // PostgreSQL allows aliasing with character literals.
  }

  @Override
  public boolean isGroupByAlias() {
    return true; // PostgreSQL allows using column aliases in GROUP BY clauses.
  }

  @Override
  public boolean isGroupByOrdinal() {
    return true; // PostgreSQL allows using ordinals (positions) in GROUP BY clauses.
  }

  @Override
  public boolean isHavingAlias() {
    return false; // PostgreSQL does not allow using aliases in HAVING clauses.
  }

  @Override
  public boolean isSortByOrdinal() {
    return false; // SQRL: issue when converting order by ordinals, SqlImplementor will convert it
    // to a char string.
    // PostgreSQL allows using ordinals in ORDER BY clauses.
  }

  @Override
  public boolean isSortByAlias() {
    return true; // PostgreSQL allows using column aliases in ORDER BY clauses.
  }

  @Override
  public boolean isSortByAliasObscures() {
    return false; // In PostgreSQL, using an alias in ORDER BY does not obscure the original column.
  }

  @Override
  public boolean isFromRequired() {
    return false; // PostgreSQL allows SELECT statements without a FROM clause.
  }

  @Override
  public boolean splitQuotedTableName() {
    return false; // PostgreSQL does not split quoted table names.
  }

  @Override
  public boolean allowHyphenInUnquotedTableName() {
    return false; // PostgreSQL does not allow hyphens in unquoted identifiers.
  }

  @Override
  public boolean isBangEqualAllowed() {
    return true; // PostgreSQL allows '!=' as an alternative to '<>'.
  }

  @Override
  public boolean isPercentRemainderAllowed() {
    return true; // PostgreSQL uses '%' for the modulo operation.
  }

  @Override
  public boolean isMinusAllowed() {
    return false; // PostgreSQL does not support the MINUS operator; it uses EXCEPT instead.
  }

  @Override
  public boolean isApplyAllowed() {
    return false; // PostgreSQL does not support APPLY operators (e.g., CROSS APPLY, OUTER APPLY).
  }

  @Override
  public boolean isInsertSubsetColumnsAllowed() {
    return true; // PostgreSQL allows inserting into a subset of columns.
  }

  @Override
  public boolean allowAliasUnnestItems() {
    return true; // PostgreSQL allows aliasing items in UNNEST (e.g., SELECT * FROM UNNEST(...) AS
    // alias).
  }

  @Override
  public boolean allowNiladicParentheses() {
    return true; // PostgreSQL requires parentheses for functions, even if they have no arguments.
  }

  @Override
  public boolean allowExplicitRowValueConstructor() {
    return true; // PostgreSQL allows explicit ROW value constructors (e.g., SELECT ROW(1, 2)).
  }

  @Override
  public boolean allowExtend() {
    return false; // PostgreSQL does not support the EXTEND keyword.
  }

  @Override
  public boolean isLimitStartCountAllowed() {
    return false; // PostgreSQL does not support 'LIMIT start, count' syntax.
  }

  @Override
  public boolean isOffsetLimitAllowed() {
    return true; // PostgreSQL supports 'LIMIT count OFFSET start' syntax.
  }

  @Override
  public boolean allowGeometry() {
    return false; // PostgreSQL does not support geometry types by default (requires PostGIS
    // extension).
  }

  @Override
  public boolean shouldConvertRaggedUnionTypesToVarying() {
    return false; // PostgreSQL does not automatically convert types in UNIONs with differing types.
  }

  @Override
  public boolean allowExtendedTrim() {
    return false; // PostgreSQL supports standard TRIM syntax.
  }

  @Override
  public boolean allowPluralTimeUnits() {
    return true; // PostgreSQL allows plural time units in INTERVAL literals (e.g., '2 days').
  }

  @Override
  public boolean allowQualifyingCommonColumn() {
    return false; // PostgreSQL does not allow qualifying columns that are common in USING clauses.
  }

  @Override
  public SqlLibrary semantics() {
    return SqlLibrary.POSTGRESQL; // Use PostgreSQL semantics.
  }

  @Override
  public boolean isValueAllowed() {
    return true;
  }

  @Override
  public boolean allowCoercionStringToArray() {
    return true;
  }
}
