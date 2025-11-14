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
package com.datasqrl.graphql.server;

import com.datasqrl.graphql.jdbc.JdbcClient;
import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import graphql.schema.DataFetcher;
import java.util.Set;
import lombok.NonNull;

//  @Value

/** Interface for context objects that provide database clients and data fetchers. */
public interface Context {

  JdbcClient getClient();

  DataFetcher<Object> createPropertyFetcher(String name);

  DataFetcher<?> createArgumentLookupFetcher(
      GraphQLEngineBuilder server, Set<Argument> arguments, ResolvedQuery resolvedQuery);

  MetadataReader getMetadataReader(@NonNull MetadataType metadataType);

  FunctionExecutor getFunctionExecutor();
}
