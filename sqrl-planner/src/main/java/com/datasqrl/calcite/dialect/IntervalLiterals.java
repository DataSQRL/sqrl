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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
final class IntervalLiterals {

  /**
   * Returns the interval literal with the unit that its value is counted in.
   *
   * <p>rel2sql prints the value of an interval literal in the unit of its SQL type and keeps the
   * qualifier the literal was written with. A WEEK interval has the type INTERVAL DAY and a QUARTER
   * interval has the type INTERVAL MONTH, so without this two weeks print as {@code INTERVAL '14'
   * WEEK}. The same holds for DECADE, CENTURY and MILLENNIUM (counted in years) and MILLISECOND
   * (counted in seconds), which {@code TIMESTAMPADD} accepts.
   */
  static SqlIntervalLiteral withValueUnit(SqlIntervalLiteral literal) {
    var interval = literal.getValueAs(SqlIntervalLiteral.IntervalValue.class);
    var qualifier = interval.getIntervalQualifier();
    var valueUnit = qualifier.typeName().getStartUnit();
    if (qualifier.getStartUnit() == valueUnit) {
      return literal;
    }
    var valueQualifier =
        new SqlIntervalQualifier(
            valueUnit,
            qualifier.getStartPrecisionPreservingDefault(),
            null,
            qualifier.getFractionalSecondPrecisionPreservingDefault(),
            qualifier.getParserPosition());
    return SqlLiteral.createInterval(
        interval.getSign(),
        interval.getIntervalLiteral(),
        valueQualifier,
        literal.getParserPosition());
  }
}
