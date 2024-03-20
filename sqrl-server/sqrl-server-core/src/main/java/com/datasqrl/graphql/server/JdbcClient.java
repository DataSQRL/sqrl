package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.Model.JdbcQuery;
import com.datasqrl.graphql.server.Model.ResolvedQuery;

public interface JdbcClient {
  ResolvedQuery prepareQuery(JdbcQuery pgQuery, Context context);
}
