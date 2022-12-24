package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.Model.PgQuery;
import com.datasqrl.graphql.server.Model.ResolvedQuery;

public interface JdbcClient {
  ResolvedQuery prepareQuery(PgQuery pgQuery, Context context);
}
