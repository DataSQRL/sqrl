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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.dialect.RedshiftSqlDialect;

/** Creates late-binding views, which Redshift requires for external tables. */
public class RedshiftCreateViewDdlFactory extends GenericCreateViewDdlFactory {

  public RedshiftCreateViewDdlFactory(ViewIdentifierResolver viewIdentifierResolver) {
    super(RedshiftSqlDialect.DEFAULT, viewIdentifierResolver);
  }

  @Override
  public String createView(SqlIdentifier viewName, List<String> columns, String select) {
    return super.createView(viewName, columns, select) + " WITH NO SCHEMA BINDING";
  }
}
