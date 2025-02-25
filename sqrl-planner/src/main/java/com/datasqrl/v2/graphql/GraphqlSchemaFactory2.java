package com.datasqrl.v2.graphql;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;
import static com.datasqrl.canonicalizer.Name.isHiddenString;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.createOutputTypeForRelDataType;
import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;
import static com.datasqrl.v2.graphql.GraphqlSchemaUtil2.*;
import static graphql.schema.GraphQLNonNull.nonNull;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.engine.server.ServerPhysicalPlan;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.v2.tables.SqrlFunctionParameter;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.v2.parser.AccessModifier;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.google.common.base.Preconditions;
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
  private final ExecutionGoal goal;
  public final Set<String> seen = new HashSet<>();

  @Inject
  public GraphqlSchemaFactory2(CompilerConfig config, ExecutionGoal goal) {
    this(config.isExtendedScalarTypes(), goal);
  }

  public GraphqlSchemaFactory2(boolean extendedScalarTypes, ExecutionGoal goal) {
    this.extendedScalarTypes = extendedScalarTypes;
    this.goal = goal;
  }

  public Optional<GraphQLSchema> generate(ServerPhysicalPlan serverPlan) {
    //configure the schema builder
    GraphQLSchema.Builder graphQLSchemaBuilder = GraphQLSchema.newSchema();
    if (extendedScalarTypes) { // use the plural parameter name in place of only bigInteger to avoid having a conf parameter of each special type mapping feature in the future
      graphQLSchemaBuilder.additionalTypes(Set.of(CustomScalars.GRAPHQL_BIGINTEGER));
    }

    // process query table functions
    final List<SqrlTableFunction> queriesTableFunctions =
        serverPlan.getFunctions().stream()
            .filter(function -> function.getVisibility().getAccess() == AccessModifier.QUERY)
            .collect(Collectors.toList());
    final Optional<GraphQLObjectType> queriesObjectType = createQueriesOrSubscriptionsObjectType(queriesTableFunctions, AccessModifier.QUERY);
    if (queryFields.isEmpty()) {
      return Optional.empty();
    }
    queriesObjectType.ifPresent(graphQLSchemaBuilder::query);

    // process subscriptions and mutations
    if (goal != ExecutionGoal.TEST) {
      // process subscriptions table functions
      final List<SqrlTableFunction> subscriptionsTableFunctions =
          serverPlan.getFunctions().stream()
              .filter(function -> function.getVisibility().getAccess() == AccessModifier.SUBSCRIPTION)
              .collect(Collectors.toList());
      final Optional<GraphQLObjectType> subscriptionsObjectType = createQueriesOrSubscriptionsObjectType(subscriptionsTableFunctions, AccessModifier.SUBSCRIPTION);
      subscriptionsObjectType.ifPresent(graphQLSchemaBuilder::subscription);

      // process mutations table functions
      final Optional<GraphQLObjectType> mutationsObjectType = createMutationsObjectType();
      mutationsObjectType.ifPresent(graphQLSchemaBuilder::mutation);
    }

    graphQLSchemaBuilder.additionalTypes(new LinkedHashSet<>(objectTypes)); // the cleaned types

    Preconditions.checkArgument(!queriesObjectType.get().getFields().isEmpty(),"No queryable tables found for server");
    return Optional.of(graphQLSchemaBuilder.build());
  }

  /**
   * GraphQL queries and subscriptions are generated the same way. So we have a single method having
   * {@link AccessModifier#QUERY} or {@link AccessModifier#SUBSCRIPTION} as parameter.
   */
  public Optional<GraphQLObjectType> createQueriesOrSubscriptionsObjectType(
      List<SqrlTableFunction> tableFunctions, AccessModifier tableFunctionsType) {
    //TODO see if the 2 maps can we simplified a bit

    // group table functions by their parent path (NamePath.ROOT  for root tables)
    Map<NamePath, List<SqrlTableFunction>> tableFunctionsByTheirParentPath =
        tableFunctions.stream()
            .collect(Collectors.groupingBy(function -> function.getFullPath().popLast(), LinkedHashMap::new, Collectors.toList()));

    // group table functions by their absolute path
    Map<NamePath, List<SqrlTableFunction>> tableFunctionsByTheirPath =
        tableFunctions.stream()
            .collect(Collectors.groupingBy(SqrlTableFunction::getFullPath, LinkedHashMap::new, Collectors.toList()));

    // process all the table functions
    for (Map.Entry<NamePath, List<SqrlTableFunction>> path : tableFunctionsByTheirPath.entrySet()) {
      // create resultType of the function at path and add it to the GraphQl object types
      final SqrlTableFunction tableFunction = path.getValue().get(0); // overloaded table functions all have the same base table, hence the same return type
      if (!tableFunction.isRelationship()) { // root table function
        Optional<GraphQLObjectType> resultType =
            createRootTableFunctionResultType(
                tableFunction, path.getValue(), // list of table functions with the same path (its overloaded table functions)
                tableFunctionsByTheirParentPath.getOrDefault(path.getKey(), List.of()) // List of table functions nested under path (its relationships).
                );
        resultType.map(objectTypes::add);
      } else { // relationship table function
        Optional<GraphQLObjectType> resultType =
            createRelationshipTableFunctionResultType(tableFunction, path.getValue() // list of table functions with the same path (its overloaded table functions)
            );
        resultType.map(objectTypes::add);
      }
    }

    GraphQLObjectType rootObjectType;
    if (tableFunctionsType == AccessModifier.QUERY) {
      final List<SqrlTableFunction> rootTableFunctions = tableFunctions.stream()
              .filter(tableFunction -> !tableFunction.isRelationship())
              .collect(Collectors.toList());
      rootObjectType = createRootQueryType(rootTableFunctions);
    } else { // subscriptions
      final List<SqrlTableFunction> rootTableFunctions = tableFunctions.stream()
              .filter(tableFunction -> !tableFunction.isRelationship())
              .collect(Collectors.toList());
      rootObjectType = createRootSubscriptionType(rootTableFunctions);
    }
    //TODO fix cleanInvalidTypes: it removes nestedTypes.
//    cleanInvalidTypes();
    return Optional.of(rootObjectType);
  }


  /**
   * Create the graphQL result type for root table functions (non-relationships)
   */
  private Optional<GraphQLObjectType> createRootTableFunctionResultType(SqrlTableFunction tableFunction, List<SqrlTableFunction> itsOverloadedTableFunctions, List<SqrlTableFunction> itsRelationships) {
    checkOverloadedFunctionsHaveSameBaseTable(itsOverloadedTableFunctions);
    checkOverloadedSignaturesDontOverlap(itsOverloadedTableFunctions);

    String typeName;
    if (tableFunction.getBaseTable().isPresent()) {
      final String baseTableName = tableFunction.getBaseTable().get().getName();
      if (definedTypeNames.contains(baseTableName)) {// result type was already defined
          return Optional.empty();
      }
      else { // new result type using base name
        typeName = baseTableName;
      }
    } else { // no base table, use function name (guaranteed to be uniq by the compiler)
      typeName = tableFunction.getFullPath().getLast().getDisplay();
    }
    /* BROWSE THE FIELDS
    They are either
      - a non-relationship field :
         - a scalar type
        - a nested relDataType (which is no more planed as a table function). For that case we stop at depth=1 for now
      - a relationship field (a table function with path size = 2) that needs to be wired up to the root table
    */

    // non-relationship fields
    // now all relationships are functions that are separate from the datatype. So there can no more have relationship fields inside the relDataType
    RelDataType rowType = tableFunction.getRowType();
    List<GraphQLFieldDefinition> fields = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      createRelDataTypeField(field).map(fields::add);
    }

    // relationship fields (reference to types defined when processing the relationship) need to be wired into the root table.
    Map<Name, List<SqrlTableFunction>> itsRelationshipsByTheirField = itsRelationships.stream()
            .collect(Collectors.groupingBy(g -> g.getFullPath().getLast(),
                    LinkedHashMap::new, Collectors.toList()));

    for (Map.Entry<Name, List<SqrlTableFunction>> field : itsRelationshipsByTheirField.entrySet()) {
      // all the overloaded relationship functions have the same return type
      SqrlTableFunction relationship = field.getValue().get(0);
      createRelationshipField(relationship).map(fields::add);
    }

    if (fields.isEmpty()) {
      return Optional.empty();
    }

    GraphQLObjectType objectType = GraphQLObjectType.newObject()
            .name(typeName)
            .fields(fields)
            .build();
    definedTypeNames.add(typeName);
    queryFields.addAll(fields);
    return Optional.of(objectType);
  }
  /**
   * Generate the result type for relationship table functions
   */
  private Optional<GraphQLObjectType> createRelationshipTableFunctionResultType(SqrlTableFunction tableFunction, List<SqrlTableFunction> itsOverloadedTableFunctions) {
    checkOverloadedFunctionsHaveSameBaseTable(itsOverloadedTableFunctions);
    checkOverloadedSignaturesDontOverlap(itsOverloadedTableFunctions);

    if (tableFunction.getBaseTable().isPresent()) { // the type was created in the root table function
      return Optional.empty();
    }
    /* BROWSE THE FIELDS
    There is No more nested relationships, so when we browse the fields, they are either
      - a scalar type
      - a nested relDataType (which is no more planed as a table function). For that case we stop at depth=1 for now
     */
    RelDataType rowType = tableFunction.getRowType();
    List<GraphQLFieldDefinition> fields = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      createRelDataTypeField(field).map(fields::add);
    }

    if (fields.isEmpty()) {
      return Optional.empty();
    }

    String typeName = uniquifyTableFunctionName(tableFunction);
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





/*
  public Optional<Builder> createSubscriptionTypes() {
    Builder subscriptionBuilder = GraphQLObjectType.newObject().name("Subscription");

    // Retrieve streamable tables from the schema
    List<PhysicalRelationalTable> streamTables = schema.getTableFunctions().stream()
        .filter(t-> t instanceof RootSqrlTable)
        .filter(t->t.getParameters().isEmpty())
        .filter(t->!((RootSqrlTable)t).isImportedTable() && !((RootSqrlTable)t).getHasExecHint())
        .map(t->((PhysicalRelationalTable)((RootSqrlTable) t).getInternalTable()))
        .filter(t->!(t instanceof ProxyImportRelationalTable)) //do not create subscriptions for imported tables
        .filter(t-> t.getType() == TableType.STREAM)
        .collect(Collectors.toList());
    if (streamTables.isEmpty()) {
      return Optional.empty();
    }

    List<GraphQLFieldDefinition> subscriptionFields = new ArrayList<>();
    // Define subscription fields for each streamable table
    outer: for (PhysicalRelationalTable table : streamTables) {
      String tableName = table.getTablePath().getDisplay();
      if (tableName.startsWith(HIDDEN_PREFIX)) continue;

      //check if primary keys are nullable:
      for (int i : table.getPrimaryKey().getPkIndexes()) {
        if (table.getRowType().getFieldList().get(i).getType().isNullable()) {
          log.warn("Nullable primary key {} on table {}", table.getRowType().getFieldList().get(i).getName(),
              table.getTableName().getDisplay());
          continue outer;
        }
      }

      GraphQLFieldDefinition subscriptionField = GraphQLFieldDefinition.newFieldDefinition()
          .name(tableName)
          .type(createOutputTypeForRelDataType(table.getRowType(), NamePath.of(tableName), seen, extendedScalarTypes).get())
          .build();

      subscriptionFields.add(subscriptionField);
    }

    if (subscriptionFields.isEmpty()) {
      return Optional.empty();
    }

    subscriptionBuilder.fields(subscriptionFields);

    return Optional.of(subscriptionBuilder);
  }
*/

  /**
   * Create the root Query graphQL object encapsulating the type references to all the root table functions.
   */
  private GraphQLObjectType createRootQueryType(List<SqrlTableFunction> rootTableFunctions) {

    List<GraphQLFieldDefinition> fields = new ArrayList<>();

    for (SqrlTableFunction tableFunction : rootTableFunctions) {
      String tableFunctionName = tableFunction.getFullPath().getDisplay();
      if (!isValidGraphQLName(tableFunctionName)) {
        continue;
      }

      GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
          .name(tableFunctionName)
          .type(wrapMultiplicity(createTypeReference(tableFunction), tableFunction.getMultiplicity()))
          .arguments(createArguments(tableFunction))
          .build();
      fields.add(field);
    }

    GraphQLObjectType rootQueryObjectType = GraphQLObjectType.newObject()
        .name("Query")
        .fields(fields)
        .build();

    definedTypeNames.add("Query");
    this.queryFields.addAll(fields);

    return rootQueryObjectType;
  }


  /**
   * Create the root Subscription graphQL object encapsulating the type references to all the root table functions.
   */
  private GraphQLObjectType createRootSubscriptionType(List<SqrlTableFunction> rootTableFunctions) {

    List<GraphQLFieldDefinition> fields = new ArrayList<>();

    for (SqrlTableFunction tableFunction : rootTableFunctions) {
      String tableFunctionName = tableFunction.getFullPath().getDisplay();
      if (!isValidGraphQLName(tableFunctionName)) {
        continue;
      }

      GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
              .name(tableFunctionName)
              .type(createTypeReference(tableFunction)) // type is nullable because there can be no update in the subscription
              .arguments(createArguments(tableFunction))
              .build();
      fields.add(field);
    }

    GraphQLObjectType rootQueryObjectType = GraphQLObjectType.newObject()
            .name("Subscription")
            .fields(fields)
            .build();

    definedTypeNames.add("Subscription");
    this.queryFields.addAll(fields);

    return rootQueryObjectType;
  }

  private static void checkOverloadedFunctionsHaveSameBaseTable(List<SqrlTableFunction> tableFunctionsAtPath) {
    SqrlTableFunction first = tableFunctionsAtPath.get(0);
    final boolean check = tableFunctionsAtPath.stream().allMatch(function -> function.getBaseTable().equals(first.getBaseTable()));
      first.getFunctionAnalysis().getErrors().checkFatal(
            check,
            "Overloaded table functions do not have the same base table: %s",
            first.getBaseTable().isEmpty()
                ? "no base table"
                : first.getBaseTable().get().getName());
  }

  private static void checkOverloadedSignaturesDontOverlap(List<SqrlTableFunction> tableFunctionsAtPath) {
    SqrlTableFunction first = tableFunctionsAtPath.get(0);
    final boolean check = true;
    //TODO compare all the signatures pairs parameters lists 2 by 2. if they have the same list of parameters (name and type) in same order they overlap
    first.getFunctionAnalysis().getErrors().checkFatal(check, "Overloaded table functions have overlapping parameters");
  }

  /**
   *   Create a non-relationship field :
   *     - a scalar type
   *     - a nested relDataType (= structured type) (which is no more planed as a table function) and which we recursively process
   *
   */
  private Optional<GraphQLFieldDefinition> createRelDataTypeField(RelDataTypeField field) {
    return getOutputType(field.getType(), NamePath.of(field.getName()), seen, extendedScalarTypes)
            .filter(type -> isValidGraphQLName(field.getName()))
            .filter(type -> isVisible(field))
            .map(type -> GraphQLFieldDefinition.newFieldDefinition()
                                            .name(field.getName())
                                            .type(wrapNullable(type, field.getType()))
                                            .build()
            );
  }

  /**
   * For a relationship table function sur as this: <pre>{@code Customer.orders := SELECT * FROM Orders o WHERE
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
        .type(wrapMultiplicity(createTypeReference(relationship), relationship.getMultiplicity()))
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
              .filter(p -> getInputType(p.getType(null), NamePath.of(p.getName()), seen, extendedScalarTypes).isPresent())
              .map(parameter -> GraphQLArgument.newArgument()
                      .name(parameter.getName())
                      .type(nonNull(getInputType(parameter.getType(null), NamePath.of(parameter.getName()), seen, extendedScalarTypes).get()))
                      .build()).collect(Collectors.toList());
    List<GraphQLArgument> limitAndOffsetArguments = List.of();
    if(tableFunction.getVisibility().getAccess() != AccessModifier.SUBSCRIPTION) {
      limitAndOffsetArguments = generateLimitAndOffsetArguments();
    }

      //TODO Merge signatures when there are multiple overloaded functions: a) combining all
    // parameters by name and relaxing their argument type by nullability b) check that argument
    // types are compatible, otherwise produce error. Also check compatibility of result type.

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
        tableFunction.getBaseTable().isPresent()
            ? tableFunction.getBaseTable().get().getName()
            : uniquifyTableFunctionName(tableFunction);
    seen.add(typeName);
    return new GraphQLTypeReference(typeName);
  }


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
