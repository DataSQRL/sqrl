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
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlIdentifier;

public class GenericCreateViewDdlFactory {

  private final DdlIdentifierQuoter identifierQuoter;
  private final ViewIdentifierResolver viewIdentifierResolver;

  public GenericCreateViewDdlFactory(
      SqlDialect dialect, ViewIdentifierResolver viewIdentifierResolver) {
    this.identifierQuoter = new DdlIdentifierQuoter(dialect);
    this.viewIdentifierResolver = viewIdentifierResolver;
  }

  public String createView(String viewName, List<String> columns, String select) {
    return createView(getViewIdentifier(viewName), columns, select);
  }

  public String createView(SqlIdentifier viewName, List<String> columns, String select) {
    var colStr = columns.stream().map(identifierQuoter::quote).collect(Collectors.joining(", "));

    return "CREATE OR REPLACE VIEW %s (%s) AS %s"
        .formatted(quoteIdentifier(viewName), colStr, select);
  }

  public SqlIdentifier getViewIdentifier(String viewName) {
    return viewIdentifierResolver.resolve(viewName);
  }

  protected String quoteIdentifier(SqlIdentifier identifier) {
    return identifier.names.stream().map(identifierQuoter::quote).collect(Collectors.joining("."));
  }
}
