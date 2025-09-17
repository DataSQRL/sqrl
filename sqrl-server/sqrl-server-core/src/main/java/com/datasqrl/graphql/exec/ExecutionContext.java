package com.datasqrl.graphql.exec;

import com.datasqrl.graphql.server.Context;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import graphql.schema.DataFetchingEnvironment;
import java.util.Set;

public interface ExecutionContext {

  Context getContext();

  DataFetchingEnvironment getEnvironment();

  Set<Argument> getArguments();
}
