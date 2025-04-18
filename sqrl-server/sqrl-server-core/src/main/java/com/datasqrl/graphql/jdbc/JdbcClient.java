package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;

public interface JdbcClient {
  ResolvedQuery prepareQuery(SqlQuery pgQuery, Context context);

  ResolvedQuery unpreparedQuery(SqlQuery sqlQuery, Context context);
}
