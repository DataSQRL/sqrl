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

import java.util.function.UnaryOperator;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.SqlToRelConverter;

public class SqrlConfigurations {

  public static final UnaryOperator<SqlWriterConfig> sqlToString =
      c ->
          c.withAlwaysUseParentheses(false)
              .withSelectListItemsOnSeparateLines(false)
              .withUpdateSetListNewline(false)
              .withIndentation(1)
              .withSelectFolding(null);

  public static final SqlToRelConverter.Config sqlToRelConverterConfig =
      SqlToRelConverter.config()
          .withExpand(false)
          .withDecorrelationEnabled(false)
          .withTrimUnusedFields(false);

  public static SqlValidator.Config sqlValidatorConfig =
      SqlValidator.Config.DEFAULT
          .withCallRewrite(true)
          .withIdentifierExpansion(false)
          .withColumnReferenceExpansion(true)
          .withTypeCoercionEnabled(true) // must be true to allow null literals
          .withLenientOperatorLookup(false)
          .withSqlConformance(SqrlConformance.INSTANCE);

  public static SqlParser.Config calciteParserConfig =
      SqlParser.config()
          .withCaseSensitive(true)
          .withConformance(SqlConformanceEnum.DEFAULT)
          .withQuoting(Quoting.DOUBLE_QUOTE);
}
