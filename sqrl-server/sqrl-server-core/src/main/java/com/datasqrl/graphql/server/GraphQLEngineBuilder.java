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

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationTypeName;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getSubscriptionTypeName;

import com.datasqrl.graphql.jdbc.AbstractQueryExecutionContext;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupQueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.FieldLookupQueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryBaseVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryCoordVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQueryVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedSqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.RootVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.SchemaVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.SqlQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
import graphql.GraphQL;
import graphql.language.FieldDefinition;
import graphql.language.InterfaceTypeDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.schema.DataFetcher;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Purpose: Builds the GraphQL engine by wiring together the schema, resolvers, and custom scalars.
 * Visits the GraphQL model to prepare the GraphQL engine for executing the requests: visits the
 * queries, the arguments, the parameters, the mutations and the subscriptions to create
 * corresponding GraphQL {@link DataFetcher}s that embed the requests execution code. {@link
 * GraphQLEngineBuilder} then registers them in the {@link GraphQLCodeRegistry}. Vert.x routes HTTP
 * requests to the GraphQLEngine. {@link GraphQLEngineBuilder} and validates the query, then
 * executes it by invoking the appropriate {@link DataFetcher}s for each field in the query.
 *
 * <p>Collaboration: Uses {@link RootGraphqlModel} to get the schema and coordinates and Context to
 * create the requests.
 */
public class GraphQLEngineBuilder
    implements RootVisitor<GraphQL.Builder, Context>,
        QueryCoordVisitor<DataFetcher<?>, Context>,
        SchemaVisitor<TypeDefinitionRegistry, Object>,
        QueryBaseVisitor<ResolvedQuery, Context>,
        ResolvedQueryVisitor<CompletableFuture, QueryExecutionContext> {

  private final List<GraphQLScalarType> extendedScalarTypes;
  private final SubscriptionConfiguration<DataFetcher<?>> subscriptionConfiguration;
  private final MutationConfiguration<DataFetcher<?>> mutationConfiguration;

  public static final ObjectTypeDefinition DUMMY_QUERY =
      ObjectTypeDefinition.newObjectTypeDefinition()
          .name("Query")
          .fieldDefinition(
              FieldDefinition.newFieldDefinition()
                  .name("_dummy")
                  .type(TypeName.newTypeName("String").build())
                  .build())
          .build();

  private GraphQLEngineBuilder(Builder builder) {
    this.extendedScalarTypes = builder.extendedScalarTypes;
    this.subscriptionConfiguration = builder.subscriptionConfiguration;
    this.mutationConfiguration = builder.mutationConfiguration;
  }

  public static class Builder {
    private List<GraphQLScalarType> extendedScalarTypes = new ArrayList<>();
    private SubscriptionConfiguration<DataFetcher<?>> subscriptionConfiguration;
    private MutationConfiguration<DataFetcher<?>> mutationConfiguration;

    public Builder withExtendedScalarTypes(List<GraphQLScalarType> types) {
      this.extendedScalarTypes = types;
      return this;
    }

    public Builder withSubscriptionConfiguration(
        SubscriptionConfiguration<DataFetcher<?>> configurer) {
      this.subscriptionConfiguration = configurer;
      return this;
    }

    public Builder withMutationConfiguration(MutationConfiguration<DataFetcher<?>> configurer) {
      this.mutationConfiguration = configurer;
      return this;
    }

    public GraphQLEngineBuilder build() {
      return new GraphQLEngineBuilder(this);
    }
  }

  @Override
  public TypeDefinitionRegistry visitStringDefinition(StringSchema stringSchema, Object context) {
    var registry = (new SchemaParser()).parse(stringSchema.getSchema());
    if (!registry.hasType(new TypeName("Query"))) {
      registry.add(DUMMY_QUERY);
    }
    return registry;
  }

  @Override
  public GraphQL.Builder visitRoot(RootGraphqlModel root, Context context) {
    var registry = root.schema.accept(this, null);

    // GraphQL registry holding the code that processes graphQL fields (graphQL DataFetchers) and
    // types (graphQL TypeResolvers)
    var codeRegistry = GraphQLCodeRegistry.newCodeRegistry();
    codeRegistry.defaultDataFetcher(
        env -> context.createPropertyFetcher(env.getFieldDefinition().getName()));
    for (QueryCoords qc : root.queries) {
      codeRegistry.dataFetcher(
          FieldCoordinates.coordinates(qc.getParentType(), qc.getFieldName()),
          qc.accept(this, context)); // creates the DataFetcher
    }

    if (root.mutations != null) {
      for (MutationCoords mc : root.mutations) {
        DataFetcher<?> fetcher =
            mc.accept(mutationConfiguration.createSinkFetcherVisitor(), context);
        codeRegistry.dataFetcher(
            FieldCoordinates.coordinates(getMutationTypeName(registry), mc.getFieldName()),
            fetcher);
      }
    }

    if (root.subscriptions != null) {
      for (SubscriptionCoords sc : root.subscriptions) {
        DataFetcher<?> subscriptionFetcher =
            sc.accept(subscriptionConfiguration.createSubscriptionFetcherVisitor(), context);
        codeRegistry.dataFetcher(
            FieldCoordinates.coordinates(getSubscriptionTypeName(registry), sc.getFieldName()),
            subscriptionFetcher);
      }
    }

    var wiring = createWiring(registry, codeRegistry);
    var graphQLSchema = new SchemaGenerator().makeExecutableSchema(registry, wiring);

    return GraphQL.newGraphQL(graphQLSchema);
  }

  private RuntimeWiring createWiring(
      TypeDefinitionRegistry registry, GraphQLCodeRegistry.Builder codeRegistry) {
    var wiring =
        RuntimeWiring.newRuntimeWiring()
            .codeRegistry(codeRegistry)
            .scalar(CustomScalars.DOUBLE)
            .scalar(CustomScalars.DATETIME)
            .scalar(CustomScalars.DATE)
            .scalar(CustomScalars.TIME)
            .scalar(CustomScalars.JSON);

    extendedScalarTypes.forEach(wiring::scalar);

    for (Map.Entry<String, TypeDefinition> typeEntry : registry.types().entrySet()) {
      if (typeEntry.getValue() instanceof InterfaceTypeDefinition) {
        // create a superficial resolver
        // TODO: interfaces and unions as return types
        wiring.type(typeEntry.getKey(), (builder) -> builder.typeResolver(env1 -> null));
      }
    }
    return wiring.build();
  }

  @Override
  public ResolvedQuery visitSqlQuery(SqlQuery query, Context context) {
    // Add pagination to query
    switch (query.pagination) {
      case NONE:
        break;
      case LIMIT_AND_OFFSET:
        // special case where database doesn't support binding for limit/offset => need to create
        // query dynamically and not prepare
        if (!query.getDatabase().supportsLimitOffsetBinding) {
          return context.getClient().unpreparedQuery(query, context);
        }
        if (query.getDatabase().supportsPositionalParameters) {
          var nextOffset = query.getParameters().size() + 1; // positional arguments are 1-based
          query =
              query.updateSQL(
                  AbstractQueryExecutionContext.addLimitOffsetToQuery(
                      query.getSql(), "$" + nextOffset, "$" + (nextOffset + 1)));
        } else {
          query =
              query.updateSQL(
                  AbstractQueryExecutionContext.addLimitOffsetToQuery(query.getSql(), "?", "?"));
        }
        break;
      default:
        throw new UnsupportedOperationException("Unsupported pagination: " + query.getPagination());
    }
    return context.getClient().prepareQuery(query, context);
  }

  @Override
  public DataFetcher<?> visitArgumentLookup(ArgumentLookupQueryCoords coords, Context ctx) {
    var query = coords.getExec().getQuery().accept(this, ctx);
    return ctx.createArgumentLookupFetcher(this, coords.getExec().getArguments(), query);
  }

  @Override
  public DataFetcher<?> visitFieldLookup(FieldLookupQueryCoords coords, Context context) {
    return context.createPropertyFetcher(coords.getColumnName());
  }

  @Override
  public CompletableFuture visitResolvedSqlQuery(
      ResolvedSqlQuery query, QueryExecutionContext context) {
    return context.runQuery(query, isList(context.getEnvironment().getFieldType()));
  }

  private boolean isList(GraphQLOutputType fieldType) {
    if (fieldType instanceof GraphQLNonNull g) {
      fieldType = (GraphQLOutputType) g.getWrappedType();
    }
    return fieldType.getClass().equals(GraphQLList.class);
  }
}
