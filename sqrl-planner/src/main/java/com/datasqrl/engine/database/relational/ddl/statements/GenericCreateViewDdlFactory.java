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
package com.datasqrl.engine.database.relational.ddl.statements;

import static com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory.quoteIdentifier;

import com.datasqrl.engine.database.relational.AbstractJdbcStatementFactory;
import java.util.List;
import java.util.stream.Collectors;

public class GenericCreateViewDdlFactory {

  public String createView(String viewName, List<String> columns, String select) {
    var colStr =
        columns.stream()
            .map(AbstractJdbcStatementFactory::quoteIdentifier)
            .collect(Collectors.joining(", "));

    return "CREATE OR REPLACE VIEW %s (%s) AS %s"
        .formatted(quoteIdentifier(viewName), colStr, select);
  }
}
