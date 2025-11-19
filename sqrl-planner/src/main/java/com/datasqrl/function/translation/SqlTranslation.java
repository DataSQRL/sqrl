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
package com.datasqrl.function.translation;

import com.datasqrl.calcite.Dialect;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;

/**
 * Used to implement a function translation to a particular dialect.
 *
 * <p>These are used to rewrite functions during unparsing in {@link
 * org.apache.calcite.sql.SqlDialect#unparseCall(SqlWriter, SqlCall, int, int)} for the various
 * dialect extensions that DataSQRL supports.
 *
 * <p>Note, that {@link SqlTranslation} is for simple function translations. If a translation
 * requires structural changes, use {@link com.datasqrl.calcite.function.OperatorRuleTransform}
 * instead.
 */
public interface SqlTranslation {

  /**
   * @return The dialect for which this translation applies
   */
  Dialect getDialect();

  /**
   * @return The operator that is translated
   */
  SqlOperator getOperator();

  /**
   * Translates the operator call via unparsing
   *
   * @param call
   * @param writer
   * @param leftPrec
   * @param rightPrec
   */
  void unparse(SqlCall call, SqlWriter writer, int leftPrec, int rightPrec);
}
