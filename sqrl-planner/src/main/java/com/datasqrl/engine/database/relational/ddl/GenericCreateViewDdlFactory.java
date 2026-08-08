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
package com.datasqrl.engine.database.relational.ddl;

import java.util.List;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.sql.SqlDialect;

@RequiredArgsConstructor
public class GenericCreateViewDdlFactory {

  private final DdlIdentifierQuoter identifierQuoter;

  public GenericCreateViewDdlFactory(SqlDialect dialect) {
    this(new DdlIdentifierQuoter(dialect));
  }

  public String createView(String viewName, List<String> columns, String select) {
    var colStr = columns.stream().map(identifierQuoter::quote).collect(Collectors.joining(", "));

    return "CREATE OR REPLACE VIEW %s (%s) AS %s"
        .formatted(identifierQuoter.quote(viewName), colStr, select);
  }
}
