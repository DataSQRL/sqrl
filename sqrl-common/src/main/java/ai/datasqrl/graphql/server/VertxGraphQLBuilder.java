package ai.datasqrl.graphql.server;

import ai.datasqrl.graphql.server.Model.Argument;
import ai.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import ai.datasqrl.graphql.server.Model.ArgumentPgParameter;
import ai.datasqrl.graphql.server.Model.CoordVisitor;
import ai.datasqrl.graphql.server.Model.Coords;
import ai.datasqrl.graphql.server.Model.FieldLookupCoords;
import ai.datasqrl.graphql.server.Model.FixedArgument;
import ai.datasqrl.graphql.server.Model.GraphQLArgumentWrapper;
import ai.datasqrl.graphql.server.Model.GraphQLArgumentWrapperVisitor;
import ai.datasqrl.graphql.server.Model.PagedPgQuery;
import ai.datasqrl.graphql.server.Model.ParameterHandlerVisitor;
import ai.datasqrl.graphql.server.Model.PgParameterHandler;
import ai.datasqrl.graphql.server.Model.PgQuery;
import ai.datasqrl.graphql.server.Model.QueryBaseVisitor;
import ai.datasqrl.graphql.server.Model.ResolvedPagedPgQuery;
import ai.datasqrl.graphql.server.Model.ResolvedPgQuery;
import ai.datasqrl.graphql.server.Model.ResolvedQuery;
import ai.datasqrl.graphql.server.Model.ResolvedQueryVisitor;
import ai.datasqrl.graphql.server.Model.Root;
import ai.datasqrl.graphql.server.Model.RootVisitor;
import ai.datasqrl.graphql.server.Model.SchemaVisitor;
import ai.datasqrl.graphql.server.Model.SourcePgParameter;
import ai.datasqrl.graphql.server.Model.StringSchema;
import ai.datasqrl.graphql.server.Model.TypeDefinitionSchema;
import ai.datasqrl.graphql.server.VertxGraphQLBuilder.QueryExecutionContext;
import ai.datasqrl.graphql.server.VertxGraphQLBuilder.VertxContext;
import com.google.common.base.Preconditions;
import graphql.GraphQL;
import graphql.schema.DataFetcher;
import graphql.schema.DataFetchingEnvironment;
import graphql.schema.FieldCoordinates;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLSchema;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.handler.graphql.schema.VertxDataFetcher;
import io.vertx.ext.web.handler.graphql.schema.VertxPropertyDataFetcher;
import io.vertx.sqlclient.PreparedQuery;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Value;

public class VertxGraphQLBuilder implements
    RootVisitor<GraphQL, VertxContext>,
    CoordVisitor<DataFetcher<?>, VertxContext>,
    SchemaVisitor<TypeDefinitionRegistry, Object>,
    GraphQLArgumentWrapperVisitor<Set<FixedArgument>, Object>,
    QueryBaseVisitor<ResolvedQuery, VertxContext>,
    ResolvedQueryVisitor<Void, QueryExecutionContext>,
    ParameterHandlerVisitor<Object, QueryExecutionContext>
{
  @Override
  public TypeDefinitionRegistry visitTypeDefinition(TypeDefinitionSchema typeDefinitionSchema,
      Object context) {
    return typeDefinitionSchema.getTypeDefinitionRegistry();
  }

  @Override
  public TypeDefinitionRegistry visitStringDefinition(StringSchema stringSchema, Object context) {
    return (new SchemaParser()).parse(stringSchema.getSchema());
  }

  @Override
  public GraphQL visitRoot(Root root, VertxContext context) {
    TypeDefinitionRegistry registry = root.schema.accept(this, null);

    GraphQLCodeRegistry.Builder codeRegistry = GraphQLCodeRegistry.newCodeRegistry();
    codeRegistry.defaultDataFetcher(env ->
        VertxPropertyDataFetcher.create(env.getFieldDefinition().getName()));
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
  public ResolvedQuery visitPgQuery(PgQuery pgQuery, VertxContext context) {
    PreparedQuery<RowSet<Row>> preparedQuery = context.getClient()
        .preparedQuery(pgQuery.getSql());
    return new ResolvedPgQuery(pgQuery, preparedQuery);
  }

  @Override
  public ResolvedQuery visitPagedPgQuery(PagedPgQuery pgQuery, VertxContext context) {
    return new ResolvedPagedPgQuery(pgQuery);
  }

  @Override
  public DataFetcher<?> visitArgumentLookup(ArgumentLookupCoords coords, VertxContext ctx) {
    //Map ResolvedQuery to precompute as much as possible
    Map<Set<Argument>, ResolvedQuery> lookupMap = coords.getMatchs().stream()
        .collect(Collectors.toMap(c->c.arguments, c->c.query.accept(this, ctx)));

    //Runtime execution, keep this as light as possible
    return VertxDataFetcher.create((env, fut) -> {
      //Map args
      Set<FixedArgument> argumentSet = GraphQLArgumentWrapper.wrap(env.getArguments())
          .accept(this, lookupMap);

      //Find query
      ResolvedQuery resolvedQuery = lookupMap.get(argumentSet);
      Preconditions.checkNotNull(resolvedQuery, "Could not find query");

      //Execute
      QueryExecutionContext context = new QueryExecutionContext(ctx,
          env, argumentSet, fut);
      resolvedQuery.accept(this, context);
    });
  }

  @Override
  public DataFetcher<?> visitFieldLookup(FieldLookupCoords coords, VertxContext context) {
    return VertxPropertyDataFetcher.create(coords.getColumnName());
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
  public Void visitResolvedPgQuery(ResolvedPgQuery pgQuery, QueryExecutionContext context) {
    Object[] paramObj = new Object[pgQuery.query.parameters.size()];
    for (int i = 0; i < pgQuery.query.getParameters().size(); i++) {
      PgParameterHandler param = pgQuery.query.getParameters().get(i);
      Object o = param.accept(this, context);
      paramObj[i] = o;
    }

    //Look at graphql response for list type here
    boolean isList = context.environment.getFieldType().getClass().equals(GraphQLList.class);

    pgQuery.getPreparedQuery().execute(Tuple.from(paramObj))
        .map(r -> resultMapper(r, isList))
        .onSuccess(context.fut::complete)
        .onFailure(f->{
          f.printStackTrace();
          context.fut.fail(f);
        });
    return null;
  }

  @Override
  public Void visitResolvedPagedPgQuery(ResolvedPagedPgQuery pgQuery, QueryExecutionContext context) {
    Optional<Integer> limit = Optional.ofNullable(context.getEnvironment().getArgument("limit"));
    Optional<Integer> offset = Optional.ofNullable(context.getEnvironment().getArgument("offset"));
    Object[] paramObj = new Object[pgQuery.query.parameters.size()];
    for (int i = 0; i < pgQuery.query.getParameters().size(); i++) {
      PgParameterHandler param = pgQuery.query.getParameters().get(i);
      Object o = param.accept(this, context);
      paramObj[i] = o;
    }

    //Look at graphql response for list type here
    boolean isList = context.environment.getFieldType().getClass().equals(GraphQLList.class);

    //Add limit + offset
    final String query = String.format("SELECT * FROM (%s) x LIMIT %s OFFSET %s",
        pgQuery.getQuery().sql,
        limit.map(Object::toString).orElse("ALL"),
        offset.orElse(0)
    );

    context.getVertxContext().getClient()
        .preparedQuery(query)
        .execute(Tuple.from(paramObj))
        .map(r -> resultMapper(r, isList))
        .onSuccess(context.fut::complete)
        .onFailure(f->{
          f.printStackTrace();
          context.fut.fail(f);
        });
    return null;
  }

  private Object resultMapper(RowSet<Row> r, boolean isList) {
    List<JsonObject> o = StreamSupport.stream(r.spliterator(), false)
        .map(Row::toJson)
        .collect(Collectors.toList());

    return isList
        ? o
        : (o.size() > 0 ? o.get(0) : null);
  }

  @Override
  public Object visitSourcePgParameter(SourcePgParameter sourceParameter, QueryExecutionContext context) {
    return VertxPropertyDataFetcher.create(sourceParameter.getKey())
        .get(context.getEnvironment());
  }

  @Override
  public Object visitArgumentPgParameter(ArgumentPgParameter argumentParameter, QueryExecutionContext context) {
    return context.getArguments().stream()
        .filter(arg -> arg.getPath().equalsIgnoreCase(argumentParameter.getPath()))
        .findFirst()
        .map(f->f.value)
        .orElse(null);
  }

  @Value
  public static class VertxContext {
    SqlClient client;
  }

  @AllArgsConstructor
  @Getter
  public static class QueryExecutionContext {
    VertxContext vertxContext;
    DataFetchingEnvironment environment;
    Set<FixedArgument> arguments;
    Promise<Object> fut;
  }
}
