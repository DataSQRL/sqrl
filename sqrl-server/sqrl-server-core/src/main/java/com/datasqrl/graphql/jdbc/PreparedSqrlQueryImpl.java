package com.datasqrl.graphql.jdbc;

import com.datasqrl.graphql.server.RootGraphqlModel.PreparedSqrlQuery;
import java.sql.Connection;
import lombok.Value;

@Value
public class PreparedSqrlQueryImpl implements PreparedSqrlQuery<String> {

  Connection connection;
  String preparedQuery;
}
