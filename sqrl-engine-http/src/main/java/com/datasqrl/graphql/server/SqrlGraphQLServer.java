/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.server;

import com.datasqrl.graphql.server.Model.Argument;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.Model.CoordVisitor;
import com.datasqrl.graphql.server.Model.Coords;
import com.datasqrl.graphql.server.Model.FieldLookupCoords;
import com.datasqrl.graphql.server.Model.FixedArgument;
import com.datasqrl.graphql.server.Model.GraphQLArgumentWrapper;
import com.datasqrl.graphql.server.Model.GraphQLArgumentWrapperVisitor;
import com.datasqrl.graphql.server.Model.PagedPgQuery;
import com.datasqrl.graphql.server.Model.PgQuery;
import com.datasqrl.graphql.server.Model.QueryBaseVisitor;
import com.datasqrl.graphql.server.Model.ResolvedPagedPgQuery;
import com.datasqrl.graphql.server.Model.ResolvedPgQuery;
import com.datasqrl.graphql.server.Model.ResolvedQuery;
import com.datasqrl.graphql.server.Model.ResolvedQueryVisitor;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.RootVisitor;
import com.datasqrl.graphql.server.Model.SchemaVisitor;
import com.datasqrl.graphql.server.Model.StringSchema;
import graphql.GraphQL;
import graphql.schema.DataFetcher;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class SqrlGraphQLServer implements
    RootVisitor<GraphQL, Context>,
    CoordVisitor<DataFetcher<?>, Context>,
    SchemaVisitor<TypeDefinitionRegistry, Object>,
    GraphQLArgumentWrapperVisitor<Set<FixedArgument>, Object>,
    QueryBaseVisitor<ResolvedQuery, Context>,
    ResolvedQueryVisitor<CompletableFuture, QueryExecutionContext> {

  @Override
  public TypeDefinitionRegistry visitStringDefinition(StringSchema stringSchema, Object context) {
    return (new SchemaParser()).parse(stringSchema.getSchema());
  }

  @Override
  public GraphQL visitRoot(RootGraphqlModel root, Context context) {
    TypeDefinitionRegistry registry = root.schema.accept(this, null);

    GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();
    codeRegistry.defaultDataFetcher(env ->
        context.createPropertyFetcher(env.getFieldDefinition().getName()));
    for (Coords c : root.coords) {
      codeRegistry.dataFetcher(
          FieldCoordinates.coordinates(c.getParentType(), c.getFieldName()),
          c.accept(this, context));
    }

    GraphQLSchema graphQLSchema = new SchemaGenerator()
        .makeExecutableSchema(registry,
            RuntimeWiring.newRuntimeWiring()
                .codeRegistry(codeRegistry).build());

    return GraphQL.newGraphQL(graphQLSchema).build();
  }

  @Override
  public ResolvedQuery visitPgQuery(PgQuery pgQuery, Context context) {
    return context.getClient()
        .prepareQuery(pgQuery, context);
  }

  @Override
  public ResolvedQuery visitPagedPgQuery(PagedPgQuery pgQuery, Context context) {
    return new ResolvedPagedPgQuery(pgQuery);
  }

  @Override
  public DataFetcher<?> visitArgumentLookup(ArgumentLookupCoords coords, Context ctx) {
    //Map ResolvedQuery to precompute as much as possible
    Map<Set<Argument>, ResolvedQuery> lookupMap = coords.getMatchs().stream()
        .collect(Collectors.toMap(c -> c.arguments, c -> c.query.accept(this, ctx)));

    //Runtime execution, keep this as light as possible
    return ctx.createArgumentLookupFetcher(this, lookupMap);
  }

  @Override
  public DataFetcher<?> visitFieldLookup(FieldLookupCoords coords, Context context) {
    return context.createPropertyFetcher(coords.getColumnName());
  }

  @Override
  public Set<FixedArgument> visitArgumentWrapper(GraphQLArgumentWrapper graphQLArgumentWrapper,
      Object context) {
    Set<FixedArgument> argumentSet = new HashSet<>(graphQLArgumentWrapper.getArgs().size());
    flattenArgs(graphQLArgumentWrapper.getArgs(), new Stack<>(), argumentSet);
    return argumentSet;
  }

  /**
   * Recursively flatten arguments
   */
  private void flattenArgs(Map<String, Object> arguments, Stack<String> names,
      Set<FixedArgument> argumentSet) {
    for (Map.Entry<String, Object> o : arguments.entrySet()) {
      names.push(o.getKey());
      if (o.getValue() instanceof Map) {
        flattenArgs((Map<String, Object>) o.getValue(), names, argumentSet);
      } else {
        String path = String.join(".", names);
        argumentSet.add(new FixedArgument(path, o.getValue()));
      }
      names.pop();
    }
  }

  @Override
  public CompletableFuture visitResolvedPgQuery(ResolvedPgQuery pgQuery,
      QueryExecutionContext context) {
    return context.runQuery(this, pgQuery, isList(context.getEnvironment().getFieldType()));
  }

  @Override
  public CompletableFuture visitResolvedPagedPgQuery(ResolvedPagedPgQuery pgQuery,
      QueryExecutionContext context) {

    return context.runPagedQuery(pgQuery, isList(context.getEnvironment().getFieldType()));
  }

  private boolean isList(GraphQLOutputType fieldType) {
    return fieldType.getClass().equals(GraphQLList.class);
  }
}
