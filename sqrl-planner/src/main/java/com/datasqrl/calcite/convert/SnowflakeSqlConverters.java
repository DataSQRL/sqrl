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
import com.datasqrl.calcite.dialect.ExtendedSnowflakeSqlDialect;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

@AutoService(SqlConverters.class)
public class SnowflakeSqlConverters extends AbstractSqlConverters {

  public SnowflakeSqlConverters() {
    super(Dialect.SNOWFLAKE, ExtendedSnowflakeSqlDialect.DEFAULT, true);
  }

  @Override
  protected SqlPrettyWriter createWriter() {
    var config =
        SqlPrettyWriter.config()
            .withDialect(getCalciteSqlDialect())
            .withQuoteAllIdentifiers(false)
            .withIndentation(0);

    return new SqlPrettyWriter(config);
  }
}
