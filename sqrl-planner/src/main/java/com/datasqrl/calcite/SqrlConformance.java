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
package com.datasqrl.calcite;

import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.validate.SqlConformance;

/** Defines the SQL standard conformance of the SqlParser and validator */
public class SqrlConformance implements SqlConformance {

  public static final SqlConformance INSTANCE = new SqrlConformance();

  @Override
  public boolean isLiberal() {
    return false;
  }

  @Override
  public boolean allowCharLiteralAlias() {
    return false;
  }

  @Override
  public boolean isGroupByAlias() {
    return true;
  }

  @Override
  public boolean isGroupByOrdinal() {
    return true;
  }

  @Override
  public boolean isHavingAlias() {
    return true;
  }

  @Override
  public boolean isSortByOrdinal() {
    return true;
  }

  @Override
  public boolean isSortByAlias() {
    return true;
  }

  /**
   * In SQL, when the ORDER BY clause is used to sort the result set, the sort order can be defined
   * by either an expression or an alias. If isSortByAliasObscures is set to true, then when an
   * alias is used to sort the result set, the sort order will be determined by the expression that
   * the alias is based on, not the alias itself.
   *
   * <p>For example, given the following query:
   *
   * <p>SELECT col1 AS a, col2 AS b FROM table ORDER BY b;
   *
   * <p>If isSortByAliasObscures is set to true, the sort order will be determined by col2, not b.
   *
   * <p>If isSortByAliasObscures is set to false, the sort order will be determined by b.
   */
  @Override
  public boolean isSortByAliasObscures() {
    return true;
  }

  @Override
  public boolean isFromRequired() {
    return true;
  }

  @Override
  public boolean splitQuotedTableName() {
    return false;
  }

  @Override
  public boolean allowHyphenInUnquotedTableName() {
    return false;
  }

  @Override
  public boolean isBangEqualAllowed() {
    return true;
  }

  @Override
  public boolean isPercentRemainderAllowed() {
    return true;
  }

  @Override
  public boolean isMinusAllowed() {
    return false;
  }

  @Override
  public boolean isApplyAllowed() {
    return false;
  }

  @Override
  public boolean isInsertSubsetColumnsAllowed() {
    return false;
  }

  @Override
  public boolean allowAliasUnnestItems() {
    return false;
  }

  @Override
  public boolean allowNiladicParentheses() {
    return true; // todo: we probably want this to be false
  }

  @Override
  public boolean allowExplicitRowValueConstructor() {
    return false;
  }

  @Override
  public boolean allowExtend() {
    return false;
  }

  @Override
  public boolean isLimitStartCountAllowed() {
    return false;
  }

  @Override
  public boolean isOffsetLimitAllowed() {
    return false;
  }

  @Override
  public boolean allowGeometry() {
    return false;
  }

  @Override
  public boolean shouldConvertRaggedUnionTypesToVarying() {
    return false;
  }

  @Override
  public boolean allowExtendedTrim() {
    return false;
  }

  @Override
  public boolean allowPluralTimeUnits() {
    return true;
  }

  @Override
  public boolean allowQualifyingCommonColumn() {
    return false;
  }

  @Override
  public SqlLibrary semantics() {
    return null;
  }

  //  @Override
  //  public SqlLibrary semantics() {
  //    return SqlConformanceEnum.DEFAULT.semantics();
  //  }
}
