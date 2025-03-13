package com.datasqrl.v2.graphql;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;
import static com.datasqrl.canonicalizer.Name.isHiddenString;
import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;
import static com.datasqrl.v2.graphql.GraphqlSchemaUtil2.*;
import static graphql.schema.GraphQLNonNull.nonNull;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.v2.analyzer.TableAnalysis;
import com.datasqrl.v2.tables.SqrlFunctionParameter;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.v2.parser.AccessModifier;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.google.inject.Inject;
import graphql.Scalars;
import graphql.language.IntValue;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.commons.collections.ListUtils;

/**
  * Creates a default graphql schema based on the SQRL schema
  */



@Slf4j
public class GraphqlSchemaFactory2 {

  private final List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
  private final List<GraphQLObjectType> objectTypes = new ArrayList<>();
  private final Set<String> definedTypeNames = new HashSet<>();
  private final boolean extendedScalarTypes;

  @Inject
  public GraphqlSchemaFactory2(CompilerConfig config) {
    this.extendedScalarTypes = config.isExtendedScalarTypes();
  }

  public Optional<GraphQLSchema> generate(ServerPhysicalPlan serverPlan) {
    //configure the schema builder
    GraphQLSchema.Builder graphQLSchemaBuilder = GraphQLSchema.newSchema();
    if (extendedScalarTypes) { // use the plural parameter name in place of only bigInteger to avoid having a conf parameter of each special type mapping feature in the future
      graphQLSchemaBuilder.additionalTypes(Set.of(CustomScalars.GRAPHQL_BIGINTEGER));
    }
    /*process table functions that are not accessible but their type might be referenced by queries or subscriptions,
      so we want to create the types but not an endpoint (i.e. we ignore the returned GraphQLObjectType)
     */
    createQueriesOrSubscriptionsObjectType(serverPlan, AccessModifier.NONE);

    // process query table functions
    final Optional<GraphQLObjectType> queriesObjectType = createQueriesOrSubscriptionsObjectType(serverPlan, AccessModifier.QUERY);
    //TODO when there is no query fields createQueriesOrSubscriptionsObjectType return an empty optional, so this test is no more neeeded.
    // and if we remove cleanInvalidTypes, the whole queryFields is no more needed.
    /*
    if (queryFields.isEmpty()) { // there must be at least 1 query
      return Optional.empty();
    }
*/
    queriesObjectType.ifPresentOrElse(
        graphQLSchemaBuilder::query,
        () -> {throw new IllegalArgumentException("No queryable tables found for server");} // there is no query
    );

    // process subscriptions table functions
    final Optional<GraphQLObjectType> subscriptionsObjectType = createQueriesOrSubscriptionsObjectType(serverPlan, AccessModifier.SUBSCRIPTION);
    subscriptionsObjectType.ifPresent(graphQLSchemaBuilder::subscription);

    // process mutations table functions
    final Optional<GraphQLObjectType> mutationsObjectType = createMutationsObjectType();
    mutationsObjectType.ifPresent(graphQLSchemaBuilder::mutation);

    graphQLSchemaBuilder.additionalTypes(new LinkedHashSet<>(objectTypes)); // the cleaned types

    return Optional.of(graphQLSchemaBuilder.build());
  }

  /**
   * GraphQL queries and subscriptions are generated the same way. So we call this method with
   * {@link AccessModifier#QUERY} for generating queries and with {@link AccessModifier#SUBSCRIPTION} for generating subscriptions.
   */
  public Optional<GraphQLObjectType> createQueriesOrSubscriptionsObjectType(ServerPhysicalPlan serverPlan, AccessModifier tableFunctionsType) {

    final List<SqrlTableFunction> tableFunctions =
            serverPlan.getFunctions().stream()
                    .filter(function -> function.getVisibility().getAccess() == tableFunctionsType)
                    .collect(Collectors.toList());

    // group table functions by their parent path
    Map<NamePath, List<SqrlTableFunction>> tableFunctionsByTheirParentPath =
        tableFunctions.stream()
            .collect(Collectors.groupingBy(
                      function -> function.getFullPath().popLast(),
                      LinkedHashMap::new,
                      Collectors.toList()
                    )
            );

    for (SqrlTableFunction tableFunction : tableFunctions) {
      NamePath typeNamePath;
      TableAnalysis tableAnalysis;
      //If the function has a base table, we need to create the type for that table instead of this function
      if (tableFunction.getFunctionAnalysis().getOptionalBaseTable().isPresent()) {
         tableAnalysis = tableFunction.getBaseTable();
         typeNamePath = NamePath.of(tableAnalysis.getName());
      } else {
        tableAnalysis = tableFunction.getFunctionAnalysis();
        typeNamePath = tableFunction.getFullPath();
      }
      Optional<GraphQLObjectType> resultType = createTableResultType(
          typeNamePath, tableAnalysis,
          tableFunctionsByTheirParentPath.getOrDefault(typeNamePath, List.of()) // List of table functions which parent is tableFunction (its relationships).
      );
      resultType.ifPresent(objectTypes::add);
    }
    // create root type ("Query" or "Subscription")
    final List<SqrlTableFunction> rootTableFunctions = tableFunctions.stream()
            .filter(tableFunction -> !tableFunction.isRelationship())
            .collect(Collectors.toList());
    Optional<GraphQLObjectType> rootObjectType = createRootType(rootTableFunctions, tableFunctionsType);
    //TODO no more needed?
    //TODO fix cleanInvalidTypes: it removes nestedTypes.
//    cleanInvalidTypes();
    return rootObjectType;
  }

  private Optional<GraphQLObjectType> createTableResultType(NamePath typePath, TableAnalysis table, List<SqrlTableFunction> itsRelationships) {
    String typeName = uniquifyNameForPath(typePath);
    if (definedTypeNames.contains(typeName)) { //If we already defined this type, move on
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
    // now all relationships are functions that are separate from the rowType. So there can no more have relationship fields inside it
    RelDataType rowType = table.getRowType();
    List<GraphQLFieldDefinition> fields = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      final NamePath fieldPath = typePath.concat(NamePath.of(field.getName()));
      createRelDataTypeField(field, fieldPath).map(fields::add);
    }

    // relationship fields if any (reference to types defined when processing the relationship) need to be wired into the root table.
    for (SqrlTableFunction relationship :  itsRelationships) {
      createRelationshipField(relationship).map(fields::add);
    }

    assert !fields.isEmpty(): "Invalid table: " + table;

    GraphQLObjectType objectType = GraphQLObjectType.newObject()
            .name(typeName)
            .fields(fields)
            .build();
    definedTypeNames.add(typeName);
    queryFields.addAll(fields);
    return Optional.of(objectType);
  }

  public Optional<GraphQLObjectType> createMutationsObjectType() {
    // TODO implement
    return Optional.empty();
  }

  /**
   * GraphQL queries and subscriptions are generated the same way. So we call this method with
   * {@link AccessModifier#QUERY} for generating root Query type and with {@link AccessModifier#SUBSCRIPTION} for generating root subscription type.
   */
  private Optional<GraphQLObjectType> createRootType(List<SqrlTableFunction> rootTableFunctions, AccessModifier tableFunctionsType) {

    List<GraphQLFieldDefinition> fields = new ArrayList<>();

    for (SqrlTableFunction tableFunction : rootTableFunctions) {
      String tableFunctionName = tableFunction.getFullPath().getDisplay();
      if (!isValidGraphQLName(tableFunctionName)) {
        continue;
      }

      final GraphQLOutputType type =
          tableFunctionsType == AccessModifier.QUERY
            ? (GraphQLOutputType) wrapMultiplicity(createTypeReference(tableFunction), tableFunction.getMultiplicity())
            : createTypeReference(tableFunction); // type is nullable because there can be no update in the subscription
      GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
          .name(tableFunctionName)
          .type(type)
          .arguments(createArguments(tableFunction))
          .build();
      fields.add(field);
    }
    if (fields.isEmpty()) {
      return Optional.empty();
    }
    String rootTypeName = tableFunctionsType.name().toLowerCase();
    rootTypeName = Character.toUpperCase(rootTypeName.charAt(0)) + rootTypeName.substring(1);
    // rootTypeName == "Query" or "Subscription"
    GraphQLObjectType rootQueryObjectType = GraphQLObjectType.newObject()
        .name(rootTypeName)
        .fields(fields)
        .build();

    definedTypeNames.add(rootTypeName);
    // TODO no more needed ?
    // for downstream cleaning invalid types
    if (tableFunctionsType == AccessModifier.QUERY) {
      this.queryFields.addAll(fields);
    }

    return Optional.of(rootQueryObjectType);
  }


  /**
   *   Create a non-relationship field :
   *     - a scalar type
   *     - a nested relDataType (= structured type) (which is no more planed as a table function) and which we recursively process
   *
   */
  private Optional<GraphQLFieldDefinition> createRelDataTypeField(RelDataTypeField field, NamePath fieldPath) {
    return getOutputType(field.getType(), fieldPath, extendedScalarTypes)
            .filter(type -> isValidGraphQLName(field.getName()))
            .filter(type -> isVisible(field))
            .map(type -> GraphQLFieldDefinition.newFieldDefinition()
                                            .name(field.getName())
                                            .type((GraphQLOutputType) wrapNullable(type, field.getType()))
                                            .build()
            );
  }

  /**
   * For a relationship table function such as this: <pre>{@code Customer.orders := SELECT * FROM Orders o WHERE
   * this.customerid = o.customerid; }</pre> Create a type reference for the orders field in the Customer table.
   */
  private Optional<GraphQLFieldDefinition> createRelationshipField(SqrlTableFunction relationship) {
    String fieldName = relationship.getFullPath().getLast().getDisplay();
    if (!isValidGraphQLName(fieldName) || isHiddenString(fieldName)) {
      return Optional.empty();
    }

    // reference the type that will be defined when the table function relationship is processed
    GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
        .name(fieldName)
        .type((GraphQLOutputType) wrapMultiplicity(createTypeReference(relationship), relationship.getMultiplicity()))
        .arguments(createArguments(relationship))
        .build();

    return Optional.of(field);
  }

  private List<GraphQLArgument> createArguments(SqrlTableFunction tableFunction) {
    if (tableFunction.getMultiplicity() != Multiplicity.MANY) {
      return List.of();
    }

    List<FunctionParameter> parameters = tableFunction.getParameters().stream()
        .filter(parameter->!((SqrlFunctionParameter)parameter).isParentField())
        .collect(Collectors.toList());

      final List<GraphQLArgument> parametersArguments = parameters.stream()
              .filter(p -> getInputType(p.getType(null), NamePath.of(p.getName()), extendedScalarTypes).isPresent())
              .map(parameter -> GraphQLArgument.newArgument()
                      .name(parameter.getName())
                      .type(nonNull(getInputType(parameter.getType(null), NamePath.of(parameter.getName()), extendedScalarTypes).get()))
                      .build()).collect(Collectors.toList());
    List<GraphQLArgument> limitAndOffsetArguments = List.of();
    if(tableFunction.getVisibility().getAccess() != AccessModifier.SUBSCRIPTION) {
      limitAndOffsetArguments = generateLimitAndOffsetArguments();
    }
    return ListUtils.union(parametersArguments, limitAndOffsetArguments);
  }

  private List<GraphQLArgument> generateLimitAndOffsetArguments() {
    GraphQLArgument limit = GraphQLArgument.newArgument()
        .name(LIMIT)
        .type(Scalars.GraphQLInt)
        .defaultValueLiteral(IntValue.of(10))
        .build();

    GraphQLArgument offset = GraphQLArgument.newArgument()
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
    String typeName =
        tableFunction.getFunctionAnalysis().getOptionalBaseTable().isPresent()
            ? tableFunction.getBaseTable().getName()
            : uniquifyNameForPath(tableFunction.getFullPath());
    return new GraphQLTypeReference(typeName);
  }


  // TODO no more needed ?
  public void cleanInvalidTypes() {
    // Ensure every field points to a valid type
    boolean found;
    int attempts = 10;
    do {
      found = false;
      Iterator<GraphQLObjectType> iterator = objectTypes.iterator();
      List<GraphQLObjectType> replacedType = new ArrayList<>();
      while (iterator.hasNext()) {
        GraphQLObjectType objectType = iterator.next();
        List<GraphQLFieldDefinition> invalidFields = new ArrayList<>();

        for (GraphQLFieldDefinition field : objectType.getFields()) {
          if (!isValidType(field.getType())) {
            invalidFields.add(field);
          }
        }

        // Refactor to remove invalid fields
        List<GraphQLFieldDefinition> fields = new ArrayList<>(objectType.getFields());
        boolean fieldsRemoved = fields.removeAll(invalidFields);

        // After removing invalid fields, if an object has no fields, it should be removed
        if (fields.isEmpty()) {
          iterator.remove();
          found = true;
        } else if (fieldsRemoved) {
          GraphQLObjectType newObjectType = GraphQLObjectType.newObject(objectType).clearFields()
              .fields(fields).build();
          replacedType.add(newObjectType);
          iterator.remove();
          found = true;
        }
      }

      //Add new types back
      objectTypes.addAll(replacedType);

      found |= queryFields.removeIf(field -> !isValidType(field.getType()));

      // Ensure each object has at least one field
      found |= objectTypes.removeIf(objectType -> objectType.getFields().isEmpty());
    } while (found && --attempts != 0);

    if (found) {
      throw new RuntimeException("Schema too complexity high, could not be reduced");
    }
  }

  boolean isValidType(GraphQLType type) {
    type = unbox(type);
    // You can expand this logic depending on the intricacies of type validation
    if (type instanceof GraphQLTypeReference) {
      GraphQLTypeReference typeReference = (GraphQLTypeReference) type;
      for (GraphQLObjectType objectType : this.objectTypes) {
        if (typeReference.getName().equalsIgnoreCase(objectType.getName())) {
          return true;
        }
      }
    }

    return isBaseGraphQLType(type);
  }

  private GraphQLType unbox(GraphQLType type) {
    if (type instanceof GraphQLNonNull) {
      return unbox(((GraphQLNonNull) type).getWrappedType());
    } else if (type instanceof GraphQLList) {
      return unbox(((GraphQLList) type).getWrappedType());
    }
    return type;
  }

  boolean isBaseGraphQLType(GraphQLType type) {
    return type instanceof GraphQLScalarType;
  }
}
