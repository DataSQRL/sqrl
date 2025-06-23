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
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverterWithHints;
import org.apache.calcite.sql.CalciteFixes;

public class SnowflakeRelToSqlNode implements RelToSqlNode {

  @Override
  public SqlNodes convert(RelNode relNode, Map<String, String> tableNameMapping) {
    var node =
        new RelToSqlConverterWithHints(ExtendedSnowflakeSqlDialect.DEFAULT, tableNameMapping)
            .visitRoot(relNode)
            .asStatement();
    CalciteFixes.appendSelectLists(node);
    return () -> node;
  }

  @Override
  public Dialect getDialect() {
    return Dialect.SNOWFLAKE;
  }
}
