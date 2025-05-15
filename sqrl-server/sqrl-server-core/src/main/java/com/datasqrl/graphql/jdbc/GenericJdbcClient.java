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
package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import java.sql.Connection;
import lombok.Value;

@Value
public class GenericJdbcClient implements JdbcClient {

  Connection connection;

  @Override
  public ResolvedQuery prepareQuery(SqlQuery pgQuery, Context context) {
    return new ResolvedSqlQuery(pgQuery, new PreparedSqrlQueryImpl(connection, pgQuery.getSql()));
  }

  @Override
  public ResolvedQuery unpreparedQuery(SqlQuery sqlQuery, Context context) {
    throw new RuntimeException("Not yet implemented");
  }
}
