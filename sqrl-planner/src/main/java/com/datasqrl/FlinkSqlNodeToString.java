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
package com.datasqrl;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.convert.RelToSqlNode.SqlNodes;
import com.datasqrl.calcite.convert.SqlNodeToString;
import com.datasqrl.engine.stream.flink.sql.RelToFlinkSql;
import com.google.auto.service.AutoService;

@AutoService(SqlNodeToString.class)
public class FlinkSqlNodeToString implements SqlNodeToString {

  @Override
  public SqlStrings convert(SqlNodes sqlNode) {
    // TODO: Migrate remaining FlinkRelToSqlConverter to this paradigm
    return RelToFlinkSql.convertToString(sqlNode);
  }

  @Override
  public Dialect getDialect() {
    return Dialect.FLINK;
  }
}
