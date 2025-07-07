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
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.server.query.ResolvedQuery;
import com.datasqrl.graphql.server.query.SqlQueryModifier;
import java.util.Optional;

public interface JdbcClient {
  ResolvedQuery prepareQuery(SqlQuery pgQuery, Context context);

  ResolvedQuery unpreparedQuery(
      SqlQuery sqlQuery, Optional<SqlQueryModifier> preprocessor, Context context);
}
