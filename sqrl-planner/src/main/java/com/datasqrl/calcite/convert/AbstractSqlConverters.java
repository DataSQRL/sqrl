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
package com.datasqrl.calcite.convert;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.DynamicParamSqlPrettyWriter;
import com.datasqrl.calcite.SqrlConfigurations;
import java.util.Map;
import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverterWithHints;
import org.apache.calcite.sql.CalciteFixes;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

@RequiredArgsConstructor(access = AccessLevel.PACKAGE)
abstract class AbstractSqlConverters implements SqlConverters {

  private final Dialect dialect;
  private final SqlDialect calciteSqlDialect;
  private final boolean appendSelectLists;

  @Override
  public final SqlNode convert(RelNode relNode, Map<String, String> tableNameMapping) {
    var sqlNode =
        new RelToSqlConverterWithHints(calciteSqlDialect, tableNameMapping)
            .visitRoot(relNode)
            .asStatement();

    if (appendSelectLists) {
      CalciteFixes.appendSelectLists(sqlNode);
    }

    return sqlNode;
  }

  @Override
  public final String convert(SqlNode sqlNode) {
    var writer = createWriter();
    sqlNode.unparse(writer, 0, 0);

    return writer.toSqlString().getSql();
  }

  @Override
  public final Dialect getDialect() {
    return dialect;
  }

  protected SqlPrettyWriter createWriter() {
    var baseConfig = SqlPrettyWriter.config().withDialect(calciteSqlDialect);
    var config = SqrlConfigurations.SQL_TO_STRING.apply(baseConfig);

    return new DynamicParamSqlPrettyWriter(config);
  }

  protected final SqlDialect getCalciteSqlDialect() {
    return calciteSqlDialect;
  }
}
