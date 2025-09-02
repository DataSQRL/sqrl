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
package com.datasqrl.graphql;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;
import static com.datasqrl.canonicalizer.Name.isHiddenString;
import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;
import static com.datasqrl.graphql.util.GraphqlSchemaUtil.getGraphQLOutputType;
import static com.datasqrl.graphql.util.GraphqlSchemaUtil.isValidGraphQLName;
import static com.datasqrl.graphql.util.GraphqlSchemaUtil.uniquifyNameForPath;
import static com.datasqrl.graphql.util.GraphqlSchemaUtil.wrapMultiplicity;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.graphql.util.GraphqlSchemaUtil;
import com.datasqrl.plan.table.Multiplicity;
import com.datasqrl.planner.analyzer.TableAnalysis;
import com.datasqrl.planner.dag.plan.MutationQuery;
import com.datasqrl.planner.parser.AccessModifier;
import com.datasqrl.planner.tables.SqrlFunctionParameter;
import com.datasqrl.planner.tables.SqrlTableFunction;
import com.datasqrl.util.CalciteUtil;
import com.google.inject.Inject;
import graphql.Scalars;
import graphql.introspection.Introspection;
import graphql.language.IntValue;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLTypeReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.commons.collections.ListUtils;

/** Creates a default graphql schema based on the SQRL schema */
@Slf4j
public class GraphqlSchemaFactory {

  private final List<GraphQLObjectType> objectTypes = new ArrayList<>();
  private final Set<String> definedTypeNames = new HashSet<>();
  private final boolean extendedScalarTypes;
  private final boolean addApiDirective;
  private final int defaultLimit;

  @Inject
  public GraphqlSchemaFactory(CompilerConfig config) {
    this.extendedScalarTypes = config.isExtendedScalarTypes();
    this.addApiDirective = !config.getApiConfig().isGraphQLProtocolOnly();
    this.defaultLimit = config.getApiConfig().getDefaultLimit();
  }

  public GraphQLSchema generate(ServerPhysicalPlan serverPlan) {
    // configure the schema builder
    var graphQLSchemaBuilder = GraphQLSchema.newSchema();
    if (extendedScalarTypes) { // use the plural parameter name in place of only bigInteger to avoid
      // having a conf parameter of each special type mapping feature in
      // the future
      CustomScalars.getExtendedScalars().forEach(graphQLSchemaBuilder::additionalType);
    }
    /*process table functions that are not accessible but their type might be referenced by queries or subscriptions,
     so we want to create the types but not an endpoint (i.e. we ignore the returned GraphQLObjectType)
    */
    createQueriesOrSubscriptionsObjectType(serverPlan, AccessModifier.NONE);

    // process query table functions
    final var queriesObjectType =
        createQueriesOrSubscriptionsObjectType(serverPlan, AccessModifier.QUERY);

    queriesObjectType.ifPresentOrElse(
        graphQLSchemaBuilder::query,
        () -> {
          throw new IllegalArgumentException("No queryable tables found in script");
        } // there is no query
        );

    // process subscriptions table functions
    final var subscriptionsObjectType =
        createQueriesOrSubscriptionsObjectType(serverPlan, AccessModifier.SUBSCRIPTION);
    subscriptionsObjectType.ifPresent(graphQLSchemaBuilder::subscription);

    // process mutations table functions
    final var mutationsObjectType = createMutationsObjectType(serverPlan.getMutations());
    mutationsObjectType.ifPresent(graphQLSchemaBuilder::mutation);

    graphQLSchemaBuilder.additionalTypes(new LinkedHashSet<>(objectTypes)); // the cleaned types

    if (addApiDirective) {
      addApiDirectiveTypes(graphQLSchemaBuilder);
    }

    return graphQLSchemaBuilder.build();
  }

  /**
   * GraphQL queries and subscriptions are generated the same way. So we call this method with
   * {@link AccessModifier#QUERY} for generating queries and with {@link
   * AccessModifier#SUBSCRIPTION} for generating subscriptions.
   */
  public Optional<GraphQLObjectType> createQueriesOrSubscriptionsObjectType(
      ServerPhysicalPlan serverPlan, AccessModifier tableFunctionsType) {

    final List<SqrlTableFunction> tableFunctions =
        serverPlan.getFunctions().stream()
            .filter(function -> function.getVisibility().access() == tableFunctionsType)
            .toList();

    // group table functions by their parent path
    Map<NamePath, List<SqrlTableFunction>> tableFunctionsByTheirParentPath =
        tableFunctions.stream()
            .collect(
                Collectors.groupingBy(
                    function -> function.getFullPath().popLast(),
                    LinkedHashMap::new,
                    Collectors.toList()));

    for (SqrlTableFunction tableFunction : tableFunctions) {
      NamePath typeNamePath;
      TableAnalysis tableAnalysis;
      // If the function has a base table, we need to create the type for that table instead of this
      // function
      if (tableFunction.getFunctionAnalysis().getOptionalBaseTable().isPresent()) {
        tableAnalysis = tableFunction.getBaseTable();
        typeNamePath = NamePath.of(tableAnalysis.getName());
      } else {
        tableAnalysis = tableFunction.getFunctionAnalysis();
        typeNamePath = tableFunction.getFullPath();
      }
      var resultType =
          createTableResultType(
              typeNamePath,
              tableAnalysis,
              tableFunctionsByTheirParentPath.getOrDefault(
                  typeNamePath,
                  List.of()) // List of table functions which parent is tableFunction (its
              // relationships).
              );
      resultType.ifPresent(objectTypes::add);
    }
    // create root type ("Query" or "Subscription")
    final List<SqrlTableFunction> rootTableFunctions =
        tableFunctions.stream()
            .filter(tableFunction -> !tableFunction.isRelationship())
            .collect(Collectors.toList());

    return createRootType(rootTableFunctions, tableFunctionsType);
  }

  private Optional<GraphQLObjectType> createTableResultType(
      NamePath typePath, TableAnalysis table, List<SqrlTableFunction> itsRelationships) {
    var typeName = uniquifyNameForPath(typePath);
    if (definedTypeNames.contains(typeName)) { // If we already defined this type, move on
      return Optional.empty();
    }
    /* BROWSE THE FIELDS
    They are either
      - a non-relationship field :
         - a scalar type
        - a nested relDataType (which is no more planed as a table function). For that case we stop at depth=1 for now
      - a relationship field (a table function with path size = 2) that needs to be wired up to the root table
    */

    // non-relationship fields
    // now all relationships are functions that are separate from the rowType. So there can no more
    // have relationship fields inside it
    var rowType = table.getRowType();
    List<GraphQLFieldDefinition> fields = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      final var fieldPath = typePath.concat(NamePath.of(field.getName()));
      createRelDataTypeField(field, fieldPath).map(fields::add);
    }

    // relationship fields if any (reference to types defined when processing the relationship) need
    // to be wired into the root table.
    for (SqrlTableFunction relationship : itsRelationships) {
      createRelationshipField(relationship).map(fields::add);
    }

    assert !fields.isEmpty() : "Invalid table: " + table;

    var objectType = GraphQLObjectType.newObject().name(typeName).fields(fields).build();
    definedTypeNames.add(typeName);
    return Optional.of(objectType);
  }

  public Optional<GraphQLObjectType> createMutationsObjectType(List<MutationQuery> mutations) {
    if (mutations.isEmpty()) {
      return Optional.empty();
    }
    var builder = GraphQLObjectType.newObject().name("Mutation");
    for (MutationQuery mutation : mutations) {
      boolean isMultiple = mutation.getInsertType().isMultiple();
      var name = mutation.getName().getDisplay();
      var inputType =
          GraphqlSchemaUtil.getGraphQLInputType(
                  mutation.getInputDataType(), NamePath.of(name), extendedScalarTypes)
              .map(
                  type ->
                      isMultiple
                          ? GraphqlSchemaUtil.asInputType(GraphqlSchemaUtil.asList(type))
                          : type)
              .orElseThrow(
                  () ->
                      new IllegalArgumentException(
                          "Could not create input type for mutation: " + mutation.getName()));
      // Create the 'event' argument which should mirror the structure of the type
      var inputArgument = GraphQLArgument.newArgument().name("event").type(inputType).build();
      var mutationFieldBuilder =
          GraphQLFieldDefinition.newFieldDefinition()
              .name(name)
              .argument(inputArgument)
              .type(
                  GraphqlSchemaUtil.getGraphQLOutputType(
                          mutation.getOutputDataType(),
                          NamePath.of(name.concat("Result")),
                          extendedScalarTypes)
                      .map(
                          type ->
                              isMultiple
                                  ? GraphqlSchemaUtil.asOutputType(GraphqlSchemaUtil.asList(type))
                                  : type)
                      .orElseThrow(
                          () ->
                              new IllegalArgumentException(
                                  "Could not create output type for mutation: "
                                      + mutation.getName())));
      mutation.getDocumentation().ifPresent(mutationFieldBuilder::description);
      builder.field(mutationFieldBuilder.build());
    }
    return Optional.of(builder.build());
  }

  /**
   * GraphQL queries and subscriptions are generated the same way. So we call this method with
   * {@link AccessModifier#QUERY} for generating root Query type and with {@link
   * AccessModifier#SUBSCRIPTION} for generating root subscription type.
   */
  private Optional<GraphQLObjectType> createRootType(
      List<SqrlTableFunction> rootTableFunctions, AccessModifier tableFunctionsType) {

    List<GraphQLFieldDefinition> fields = new ArrayList<>();

    for (SqrlTableFunction tableFunction : rootTableFunctions) {
      var tableFunctionName = tableFunction.getFullPath().getDisplay();
      if (!isValidGraphQLName(tableFunctionName)) {
        continue;
      }

      final var type =
          tableFunctionsType == AccessModifier.QUERY
              ? (GraphQLOutputType)
                  wrapMultiplicity(
                      createTypeReference(tableFunction), tableFunction.getMultiplicity())
              : createTypeReference(
                  tableFunction); // type is nullable because there can be no update in the
      // subscription
      var fieldBuilder =
          GraphQLFieldDefinition.newFieldDefinition()
              .name(tableFunctionName)
              .type(type)
              .arguments(createArguments(tableFunction));
      tableFunction.getDocumentation().ifPresent(fieldBuilder::description);
      fields.add(fieldBuilder.build());
    }
    if (fields.isEmpty()) {
      return Optional.empty();
    }
    var rootTypeName = tableFunctionsType.name().toLowerCase();
    rootTypeName = Character.toUpperCase(rootTypeName.charAt(0)) + rootTypeName.substring(1);
    // rootTypeName == "Query" or "Subscription"
    var rootQueryObjectType =
        GraphQLObjectType.newObject().name(rootTypeName).fields(fields).build();

    definedTypeNames.add(rootTypeName);

    return Optional.of(rootQueryObjectType);
  }

  /**
   * Create a non-relationship field : - a scalar type - a nested relDataType (= structured type)
   * (which is no more planed as a table function) and which we recursively process
   */
  private Optional<GraphQLFieldDefinition> createRelDataTypeField(
      RelDataTypeField field, NamePath fieldPath) {
    return getGraphQLOutputType(field.getType(), fieldPath, extendedScalarTypes)
        .filter(type -> isValidGraphQLName(field.getName()))
        .filter(type -> isVisible(field))
        .map(
            type ->
                GraphQLFieldDefinition.newFieldDefinition()
                    .name(field.getName())
                    .type(type)
                    .build());
  }

  /**
   * For a relationship table function such as this:
   *
   * <pre>{@code Customer.orders := SELECT * FROM Orders o WHERE
   * this.customerid = o.customerid; }</pre>
   *
   * Create a type reference for the orders field in the Customer table.
   */
  private Optional<GraphQLFieldDefinition> createRelationshipField(SqrlTableFunction relationship) {
    var fieldName = relationship.getFullPath().getLast().getDisplay();
    if (!isValidGraphQLName(fieldName) || isHiddenString(fieldName)) {
      return Optional.empty();
    }

    // reference the type that will be defined when the table function relationship is processed
    var field =
        GraphQLFieldDefinition.newFieldDefinition()
            .name(fieldName)
            .type(
                (GraphQLOutputType)
                    wrapMultiplicity(
                        createTypeReference(relationship), relationship.getMultiplicity()))
            .arguments(createArguments(relationship))
            .build();

    return Optional.of(field);
  }

  private List<GraphQLArgument> createArguments(SqrlTableFunction tableFunction) {
    List<FunctionParameter> parameters =
        tableFunction.getParameters().stream()
            .filter(parameter -> ((SqrlFunctionParameter) parameter).isExternalArgument())
            .toList();

    final List<GraphQLArgument> parametersArguments =
        parameters.stream()
            .filter(
                p ->
                    GraphqlSchemaUtil.getGraphQLInputType(
                            p.getType(null), NamePath.of(p.getName()), extendedScalarTypes)
                        .isPresent())
            .map(
                parameter ->
                    GraphQLArgument.newArgument()
                        .name(parameter.getName())
                        .type(
                            GraphqlSchemaUtil.getGraphQLInputType(
                                    parameter.getType(null),
                                    NamePath.of(parameter.getName()),
                                    extendedScalarTypes)
                                .get())
                        .build())
            .collect(Collectors.toList());
    List<GraphQLArgument> limitAndOffsetArguments = List.of();
    if (tableFunction.getVisibility().access() != AccessModifier.SUBSCRIPTION
        && tableFunction.getMultiplicity() == Multiplicity.MANY) {
      limitAndOffsetArguments = generateLimitAndOffsetArguments(tableFunction);
    }
    return ListUtils.union(parametersArguments, limitAndOffsetArguments);
  }

  private List<GraphQLArgument> generateLimitAndOffsetArguments(SqrlTableFunction tableFunction) {
    var limitValue = tableFunction.getLimit().orElse(defaultLimit);
    var limit =
        GraphQLArgument.newArgument()
            .name(LIMIT)
            .type(Scalars.GraphQLInt)
            .defaultValueLiteral(IntValue.of(limitValue))
            .build();

    var offset =
        GraphQLArgument.newArgument()
            .name(OFFSET)
            .type(Scalars.GraphQLInt)
            .defaultValueLiteral(IntValue.of(0))
            .build();
    return List.of(limit, offset);
  }

  private boolean isVisible(RelDataTypeField f) {
    return !f.getName().startsWith(HIDDEN_PREFIX);
  }

  private GraphQLOutputType createTypeReference(SqrlTableFunction tableFunction) {
    var typeName =
        tableFunction.getFunctionAnalysis().getOptionalBaseTable().isPresent()
            ? tableFunction.getBaseTable().getName()
            : uniquifyNameForPath(tableFunction.getFullPath());
    return new GraphQLTypeReference(typeName);
  }

  public static final String API_DIRECTIVE_NAME = "api";

  private void addApiDirectiveTypes(GraphQLSchema.Builder graphQLSchemaBuilder) {
    var mcpMethodType =
        GraphQLEnumType.newEnum()
            .name("_McpMethodType")
            .value("NONE")
            .value("TOOL")
            .value("RESOURCE")
            .build();

    var restMethodType =
        GraphQLEnumType.newEnum()
            .name("_RestMethodType")
            .value("NONE")
            .value("GET")
            .value("POST")
            .build();

    var apiDirective =
        GraphQLDirective.newDirective()
            .name(API_DIRECTIVE_NAME)
            .validLocations(
                Introspection.DirectiveLocation.FIELD_DEFINITION,
                Introspection.DirectiveLocation.QUERY,
                Introspection.DirectiveLocation.MUTATION)
            .argument(
                GraphQLArgument.newArgument()
                    .name("mcp")
                    .type(new GraphQLTypeReference("_McpMethodType")))
            .argument(
                GraphQLArgument.newArgument()
                    .name("rest")
                    .type(new GraphQLTypeReference("_RestMethodType")))
            .argument(GraphQLArgument.newArgument().name("uri").type(Scalars.GraphQLString))
            .build();

    graphQLSchemaBuilder.additionalType(mcpMethodType);
    graphQLSchemaBuilder.additionalType(restMethodType);
    graphQLSchemaBuilder.additionalDirective(apiDirective);
  }
}
