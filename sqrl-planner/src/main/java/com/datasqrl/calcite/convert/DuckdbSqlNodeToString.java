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
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.dialect.DuckDbSqlDialect;
import com.google.auto.service.AutoService;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

@AutoService(SqlNodeToString.class)
public class DuckdbSqlNodeToString implements SqlNodeToString {

  @Override
  public SqlStrings convert(SqlNodes sqlNode) {
    var config =
        SqrlConfigurations.sqlToString.apply(
            SqlPrettyWriter.config().withDialect(DuckDbSqlDialect.DEFAULT));
    var writer = new DynamicParamSqlPrettyWriter(config);
    sqlNode.getSqlNode().unparse(writer, 0, 0);
    return () -> writer.toSqlString().getSql();
  }

  @Override
  public Dialect getDialect() {
    return Dialect.DUCKDB;
  }
}
