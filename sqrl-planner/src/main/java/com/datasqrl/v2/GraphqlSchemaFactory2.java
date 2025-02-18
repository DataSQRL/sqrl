package com.datasqrl.v2;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;
import static com.datasqrl.canonicalizer.Name.isHiddenString;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.createOutputTypeForRelDataType;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.getInputType;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.getOutputType;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.isValidGraphQLName;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.wrapMultiplicity;
import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;
import static graphql.schema.GraphQLNonNull.nonNull;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.graphql.generate.GraphqlSchemaUtil;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.NestedRelationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.RootSqrlTable;
import com.datasqrl.v2.dag.plan.ServerStagePlan;
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
import graphql.schema.GraphQLObjectType.Builder;
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
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.ListUtils;

 /**
  * Creates a default graphql schema based on the SQRL schema
  */



@Slf4j
public class GraphqlSchemaFactory2 {

  private final List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
  private final List<GraphQLObjectType> objectTypes = new ArrayList<>();
  private final Set<String> usedNames = new HashSet<>();
  private final boolean addArguments;
  private final boolean extendedScalarTypes;
  private final LogManager logManager;
  private final ExecutionGoal goal;
  private final ServerStagePlan serverStagePlan;
    private Map<NamePath, List<SqrlTableFunction>> absolutePathToTableFunctions;
  public final Set<String> seen = new HashSet<>();

  @Inject
  public GraphqlSchemaFactory2(CompilerConfig config, LogManager logManager, ExecutionGoal goal, ServerStagePlan serverStagePlan) {
    this(config.isAddArguments(), config.isExtendedScalarTypes(), logManager, goal, serverStagePlan);
  }

  public GraphqlSchemaFactory2(boolean addArguments, boolean extendedScalarTypes, LogManager logManager, ExecutionGoal goal, ServerStagePlan serverStagePlan) {
    this.addArguments = addArguments;
    this.logManager = logManager;
    this.extendedScalarTypes = extendedScalarTypes;
    this.goal = goal;
    this.serverStagePlan = serverStagePlan;
  }

  public Optional<GraphQLSchema> generate() {
    //configure the schema builder
    GraphQLSchema.Builder graphQLSchemaBuilder = GraphQLSchema.newSchema();
    if (extendedScalarTypes) { // use the plural parameter name in place of only bigInteger to avoid having a conf parameter of each special type mapping feature in the future
      graphQLSchemaBuilder.additionalTypes(Set.of(CustomScalars.GRAPHQL_BIGINTEGER));
    }

    // process query table functions
    final List<SqrlTableFunction> queriesTableFunctions =
        serverStagePlan.getFunctions().stream()
            .filter(
                function ->
                    function.getVisibility().getAccess() == AccessModifier.QUERY)
            .collect(Collectors.toList());
    final Optional<GraphQLObjectType> queriesObjectType = createQueriesOrSubscriptionsObjectType(queriesTableFunctions, AccessModifier.QUERY);
    if (queryFields.isEmpty()) {
      return Optional.empty();
    }
    queriesObjectType.ifPresent(graphQLSchemaBuilder::query);

    // process subscriptions and mutations
    if (goal != ExecutionGoal.TEST) {
      if (logManager.hasLogEngine() && System.getenv().get("ENABLE_SUBSCRIPTIONS") != null) {
        // process subscriptions table functions
        final List<SqrlTableFunction> subscriptionsTableFunctions =
            serverStagePlan.getFunctions().stream()
                .filter(
                    function -> function.getVisibility().getAccess() == AccessModifier.SUBSCRIPTION)
                .collect(Collectors.toList());
        final Optional<GraphQLObjectType> subscriptionsObjectType =
            createQueriesOrSubscriptionsObjectType(subscriptionsTableFunctions, AccessModifier.SUBSCRIPTION);
        subscriptionsObjectType.ifPresent(graphQLSchemaBuilder::subscription);
      }

      // process mutations
      final Optional<GraphQLObjectType> mutationsObjectType = createMutationsObjectType();
      mutationsObjectType.ifPresent(graphQLSchemaBuilder::mutation);
    }

    graphQLSchemaBuilder.additionalTypes(new LinkedHashSet<>(objectTypes)); // the cleaned types

    //TODO there might be a pb in the original program flow: why fields (before pruning) at the end while this.queryFields empty is tested just after invalid fields pruning
    if (queriesObjectType.get().getFields().isEmpty()) {
      if (goal == ExecutionGoal.TEST) {
        return Optional.empty(); //may have test folder
      } else {
        throw new RuntimeException("No queryable tables found for server");
      }
    }
    return Optional.of(graphQLSchemaBuilder.build());
  }

    public Optional<GraphQLObjectType> createQueriesOrSubscriptionsObjectType(List<SqrlTableFunction> tableFunctions, AccessModifier tableFunctionsType) {
    // group table functions by their parent path
        Map<NamePath, List<SqrlTableFunction>> parentToTableFunctions = tableFunctions.stream()
                .collect(Collectors.groupingBy(function -> function.getFullPath().popLast(),
                        LinkedHashMap::new, Collectors.toList()));

    // group table functions by their absolute path
    this.absolutePathToTableFunctions = tableFunctions.stream()
        .collect(Collectors.groupingBy(SqrlTableFunction::getFullPath,
            LinkedHashMap::new, Collectors.toList()));

    for (Map.Entry<NamePath, List<SqrlTableFunction>> path : absolutePathToTableFunctions.entrySet()) { // TODO do not use a field because it will be used for both queries and subscriptions.
      // TODO, in this loop we have only the path to identify a given function, can we remove the hidden functions at absolutePathToTableFunctions creation time using AccessVisibility?
      if (path.getKey().getLast().isHidden()) continue; // filter out the hidden table functions

      // walk resulType of the function at path and add it to the GraphQl object types
      Optional<GraphQLObjectType> graphQLObjectType = createFunctionResultType(path.getValue(), // list of table functions with the same path
          parentToTableFunctions.getOrDefault(path.getKey(), List.of())); // List of table functions nested under path
      graphQLObjectType.map(objectTypes::add);
    }

      GraphQLObjectType objectType;
    if (tableFunctionsType == AccessModifier.QUERY) {
      objectType = createRootQueryType(parentToTableFunctions.get(NamePath.ROOT));
    } else { // subscriptions
      objectType = null; //TODO implement
    }
    //TODO there might be a pb in the original program flow: invalid fields pruning is done after queryType creation
    cleanInvalidTypes();
    return Optional.of(objectType);
  }


  public Optional<GraphQLObjectType> createMutationsObjectType() {
    // TODO implement
    return Optional.empty();
  }





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


  private GraphQLObjectType createRootQueryType(List<SqrlTableFunction> rootTables) {

    List<GraphQLFieldDefinition> fields = new ArrayList<>();

    for (SqrlTableFunction rootTable : rootTables) {
      String name = rootTable.getFullPath().getDisplay();
      if (name.startsWith(HIDDEN_PREFIX) || !isValidGraphQLName(name)) continue;

      if (goal == ExecutionGoal.TEST) {
        if (!rootTable.getVisibility().isTest()) continue;
      } else {
        if (rootTable.getVisibility().isTest()) continue;
      }

      GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
          .name(rootTable.getFullPath().getDisplay())
          .type(wrapMultiplicity(createTypeReference(rootTable.getFullPath().getLast().getDisplay()), rootTable.getMultiplicity()))
          .arguments(createArguments(rootTable))
          .build();
      fields.add(field);
    }

    GraphQLObjectType rootQueryObjectType = GraphQLObjectType.newObject()
        .name("Query")
        .fields(fields)
        .build();

    usedNames.add("Query");

    this.queryFields.addAll(fields);

    return rootQueryObjectType;
  }

  private Optional<GraphQLObjectType> createFunctionResultType(List<SqrlTableFunction> tableFunctionsAtPath, List<SqrlTableFunction> nestedTableFunctionsUnderPath) {
    //TODO check overloaded functions have the same basetable
    //TODO check that overloaded functions' signatures don't overlap (2 functions with same signatures)
    //TODO maybe in the future go deeper that level one of nested fields


    // table functions can be overloaded (hence the list in value) but they all have the same return type (hence taking the first one)
    SqrlTableFunction first = tableFunctionsAtPath.get(0);
    RelDataType rowType = first.getRowType();

    Map<Name, List<SqrlTableFunction>> nestedTableFunctionsUnderPathByField = nestedTableFunctionsUnderPath.stream()
            .collect(Collectors.groupingBy(g -> g.getFullPath().getLast(),
                    LinkedHashMap::new, Collectors.toList()));

    // create the graphQL fields for non-relationship fields
    List<GraphQLFieldDefinition> fields = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (!nestedTableFunctionsUnderPathByField.containsKey(Name.system(field.getName()))) {
          createSimpleGraphQlField(field).map(fields::add);
      }
    }
    // create the graphQL fields for relationship fields
    for (Map.Entry<Name, List<SqrlTableFunction>> name : nestedTableFunctionsUnderPathByField.entrySet()) {
      createNestedGraphQlField(name.getValue()).map(fields::add);
    }

    if (fields.isEmpty()) {
      return Optional.empty();
    }

    String name = generateObjectName(first.getFullPath());
    GraphQLObjectType objectType = GraphQLObjectType.newObject()
        .name(name)
        .fields(fields)
        .build();
    usedNames.add(name);
    queryFields.addAll(fields);
    return Optional.of(objectType);
  }

  private String uniquifyName(String name) {
    while (usedNames.contains(name)) {
      name = name + "_"; //add suffix
    }
    return name;
  }

  private String generateObjectName(NamePath path) {
    if (path.isEmpty()) {
      return "Query";
    }

    return uniquifyName(path.getLast().getDisplay());
  }

  //TODO create a  method that takes either a relDataType or a table function. before everything was "casted" to a table function, now nested types are nested relDataTypes.
  private Optional<GraphQLFieldDefinition> createSimpleGraphQlField(RelDataTypeField field) {
    return getOutputType(field.getType(), NamePath.of(field.getName()), seen, extendedScalarTypes)
            .filter(type ->isValidGraphQLName(field.getName()))
            .filter(type ->isVisible(field))
            .map(type -> GraphQLFieldDefinition.newFieldDefinition()
                                            .name(field.getName())
                                            .type(GraphqlSchemaUtil.wrapNullable(type, field.getType().isNullable())).build());
  }

  private Optional<GraphQLFieldDefinition> createNestedGraphQlField(List<SqrlTableFunction> tableFunctionsWithSameParent) {
    SqrlTableFunction tableFunction = tableFunctionsWithSameParent.get(0); // all the overloaded functions have the same return type
    String fieldName = tableFunction.getFullPath().getLast().getDisplay();
    if (!isValidGraphQLName(fieldName) || isHiddenString(fieldName)) {
      return Optional.empty();
    }

    GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
        .name(fieldName)
        .type(wrapMultiplicity(createTypeReference(fieldName), tableFunction.getMultiplicity()))
        .arguments(createArguments(tableFunction))
        .build();

    return Optional.of(field);
  }

  private List<GraphQLArgument> createArguments(SqrlTableFunction tableFunction) {
    // TODO allowed only if multiplicity == many and joinType != parent. We no more have joinType

    List<FunctionParameter> parameters = tableFunction.getParameters().stream()
        .filter(parameter->!((SqrlFunctionParameter)parameter).isInternal())
        .collect(Collectors.toList());

      // TODO what is the addArguments config parameter (in the current code, the processing below is done even if addArguments is false)
      final List<GraphQLArgument> arguments = parameters.stream()
              .filter(p -> getInputType(p.getType(null), NamePath.of(p.getName()), seen, extendedScalarTypes).isPresent())
              .map(parameter -> GraphQLArgument.newArgument()
                      .name(((SqrlFunctionParameter) parameter).getVariableName())
                      .type(nonNull(getInputType(parameter.getType(null), NamePath.of(parameter.getName()), seen, extendedScalarTypes).get()))
                      .build()).collect(Collectors.toList());
      List<GraphQLArgument> limitOffset = generateLimitOffset();

    // TODO Merge signatures when there are multiple overloaded functions: a) combining all
    // parameters by name and relaxing their argument type by nullability b) check that argument
    // types are compatible, otherwise produce error. Also check compatibility of result type.

    return ListUtils.union(arguments, limitOffset);
  }

  private List<GraphQLArgument> generatePermuted(SqrlTableMacro macro) {
    if (macro instanceof NestedRelationship) return List.of();
    String tableName = schema.getPathToSysTableMap().get(macro.getAbsolutePath());
    if (tableName == null) {
      log.info("Table name null, to debug");
      return List.of();
    }
    Table table = schema.getTable(tableName, false).getTable();
    List<ImmutableBitSet> keys = table.getStatistic().getKeys();
    if (keys.isEmpty()) return List.of(); //no pks
    ImmutableBitSet pks = keys.get(0);
    RelDataType rowType = macro.getRowType();
    List<RelDataTypeField> primaryKeys = new ArrayList<>();
    for (Integer key : pks.asList()) {
      primaryKeys.add(rowType.getFieldList().get(key));
    }

    return primaryKeys
            .stream()
            .filter(f -> getInputType(f.getType(), NamePath.of(tableName), seen, extendedScalarTypes).isPresent())
            .filter(f -> isValidGraphQLName(f.getName()))
            .filter(this::isVisible)
            .map(f -> GraphQLArgument.newArgument()
                    .name(f.getName())
                    .type(getInputType(f.getType(), NamePath.of(f.getName()), seen, extendedScalarTypes).get())
                    .build())
            .collect(Collectors.toList());
  }

  private boolean allowedArguments(SqrlTableFunction tableFunction) {
    //No arguments for to-one rels or parent fields
    return tableFunction.getMultiplicity().equals(Multiplicity.MANY) &&
        !tableFunction.getJoinType().equals(JoinType.PARENT);
  }



  private List<GraphQLArgument> generateLimitOffset() {

    //add limit / offset
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

  private GraphQLOutputType createTypeReference(String tableFunctionName) {
    seen.add(tableFunctionName);

    return new GraphQLTypeReference(tableFunctionName);
    //TODO if it is a table function: the type reference is the name of base table if exists because when the table function has a base table, its return type is the one of the base table. To avoid creating as many graphQl types as functions return types, we create a type for the base table when the base table exists otherwise we create a type with the name of the table function.
    // if it is a reldatatype: the type name is the name of the field
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
