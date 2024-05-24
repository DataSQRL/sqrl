/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.server;

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationTypeName;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getSubscriptionTypeName;

import com.datasqrl.graphql.server.RootGraphqlModel.Argument;
import com.datasqrl.graphql.server.RootGraphqlModel.ArgumentLookupCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.CoordVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.FieldLookupCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.JdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.MutationCoords;
import com.datasqrl.graphql.server.RootGraphqlModel.PagedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.QueryBaseVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.Coords;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedPagedJdbcQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQuery;
import com.datasqrl.graphql.server.RootGraphqlModel.ResolvedQueryVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.RootVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.SchemaVisitor;
import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
import com.datasqrl.graphql.server.RootGraphqlModel.SubscriptionCoords;
import graphql.GraphQL;
import graphql.GraphQL.Builder;
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
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class GraphQLEngineBuilder implements
    RootVisitor<Builder, Context>,
    CoordVisitor<DataFetcher<?>, Context>,
    SchemaVisitor<TypeDefinitionRegistry, Object>,
    QueryBaseVisitor<ResolvedQuery, Context>,
    ResolvedQueryVisitor<Object, QueryExecutionContext> {

  private List<GraphQLScalarType> addlTypes;

  public static final ObjectTypeDefinition DUMMY_QUERY = ObjectTypeDefinition.newObjectTypeDefinition()
      .name("Query")
      .fieldDefinition(FieldDefinition.newFieldDefinition()
          .name("_dummy")
          .type(TypeName.newTypeName("String").build())
          .build())
      .build();

  public GraphQLEngineBuilder() {
    addlTypes = new ArrayList<>();
  }

  public GraphQLEngineBuilder(List<GraphQLScalarType> addlTypes) {
    this.addlTypes = addlTypes;
  }

  @Override
  public TypeDefinitionRegistry visitStringDefinition(StringSchema stringSchema, Object context) {
    TypeDefinitionRegistry registry = (new SchemaParser()).parse(stringSchema.getSchema());
    if (!registry.hasType(new TypeName("Query"))) {
      registry.add(DUMMY_QUERY);
    }
    return registry;
  }

  @Override
  public Builder visitRoot(RootGraphqlModel root, Context context) {
    TypeDefinitionRegistry registry = root.schema.accept(this, null);

    GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();
    codeRegistry.defaultDataFetcher(env ->
        context.createPropertyFetcher(env.getFieldDefinition().getName()));
    for (Coords qc : root.coords) {
      codeRegistry.dataFetcher(
          FieldCoordinates.coordinates(qc.getParentType(), qc.getFieldName()),
          qc.accept(this, context));
    }

    if (root.mutations != null) {
      for (MutationCoords mc : root.mutations) {
        codeRegistry.dataFetcher(
            FieldCoordinates.coordinates(getMutationTypeName(registry), mc.getFieldName()),
            context.createSinkFetcher(mc));
      }
    }

    if (root.subscriptions != null) {
      for (SubscriptionCoords sc : root.subscriptions) {
        codeRegistry.dataFetcher(
            FieldCoordinates.coordinates(getSubscriptionTypeName(registry), sc.getFieldName()),
            context.createSubscriptionFetcher(sc, sc.filters));
      }
    }

    RuntimeWiring wiring = createWiring(registry, codeRegistry);

    GraphQLSchema graphQLSchema = new SchemaGenerator()
        .makeExecutableSchema(registry, wiring);

    return GraphQL.newGraphQL(graphQLSchema);
  }

  private RuntimeWiring createWiring(TypeDefinitionRegistry registry, GraphQLCodeRegistry.Builder codeRegistry) {
    RuntimeWiring.Builder wiring = RuntimeWiring.newRuntimeWiring()
        .codeRegistry(codeRegistry)
        .scalar(CustomScalars.Double)
        .scalar(CustomScalars.DATETIME);

    addlTypes.forEach(t->wiring.scalar(t));

    for (Map.Entry<String, TypeDefinition> typeEntry : registry.types().entrySet()) {
      if (typeEntry.getValue() instanceof InterfaceTypeDefinition) {
        //create a superficial resolver
        //TODO: interfaces and unions as return types
        wiring.type(typeEntry.getKey(), (builder)-> builder
            .typeResolver(env1 -> null));
      }
    }
    return wiring.build();
  }

  @Override
  public ResolvedQuery visitJdbcQuery(JdbcQuery jdbcQuery, Context context) {
    return context.getClient()
        .prepareQuery(jdbcQuery, context);
  }

  @Override
  public ResolvedQuery visitPagedJdbcQuery(PagedJdbcQuery jdbcQuery, Context context) {
    return new ResolvedPagedJdbcQuery(jdbcQuery);
  }

  @Override
  public DataFetcher<?> visitArgumentLookup(ArgumentLookupCoords coords, Context ctx) {
    //Map ResolvedQuery to precompute as much as possible
    Map<Set<Argument>, ResolvedQuery> lookupMap = coords.getMatchs().stream()
        .collect(Collectors.toMap(c -> c.arguments, c -> c.query.accept(this, ctx)));

    //Runtime execution, keep this as light as possible
    DataFetcher fetcher = ctx.createArgumentLookupFetcher(this, lookupMap);
    return fetcher;
  }

  @Override
  public DataFetcher<?> visitFieldLookup(FieldLookupCoords coords, Context context) {
    return context.createPropertyFetcher(coords.getColumnName());
  }

  @Override
  public Object visitResolvedJdbcQuery(ResolvedJdbcQuery query,
      QueryExecutionContext context) {
    return context.runQuery(this, query, isList(context.getEnvironment().getFieldType()));
  }

  @Override
  public Object visitResolvedPagedJdbcQuery(ResolvedPagedJdbcQuery query,
      QueryExecutionContext context) {
    return context.runPagedJdbcQuery(query,
        isList(context.getEnvironment().getFieldType()),
        context);
  }

  private boolean isList(GraphQLOutputType fieldType) {
    if (fieldType instanceof GraphQLNonNull) {
      fieldType = (GraphQLOutputType)((GraphQLNonNull) fieldType).getWrappedType();
    }
    return fieldType.getClass().equals(GraphQLList.class);
  }

}
