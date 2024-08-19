/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;
import static com.datasqrl.canonicalizer.Name.isHiddenString;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.createOutputTypeForRelDataType;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.getInputType;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.getOutputType;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.isValidGraphQLName;
import static com.datasqrl.graphql.generate.GraphqlSchemaUtil.wrap;
import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;
import static graphql.schema.GraphQLNonNull.nonNull;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;

import com.datasqrl.config.PackageJson.CompilerConfig;
import com.datasqrl.config.SystemBuiltInConnectors;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.function.SqrlFunctionParameter;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.io.tables.TableType;
import com.datasqrl.plan.table.PhysicalRelationalTable;
import com.datasqrl.plan.table.ProxyImportRelationalTable;
import com.datasqrl.plan.table.QueryRelationalTable;
import com.datasqrl.plan.validate.ExecutionGoal;
import com.datasqrl.plan.validate.ResolvedImport;
import com.datasqrl.plan.validate.ScriptPlanner.Mutation;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.NestedRelationship;
import com.datasqrl.schema.Relationship.JoinType;
import com.datasqrl.schema.RootSqrlTable;
import com.google.inject.Inject;
import graphql.Scalars;
import graphql.language.IntValue;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputType;
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
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Table;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.ListUtils;
import scala.annotation.meta.field;

/**
 * Creates a default graphql schema based on the SQRL schema
 */
@Slf4j
public class GraphqlSchemaFactory {

  private final List<GraphQLFieldDefinition> queryFields = new ArrayList<>();
  private final List<GraphQLObjectType> objectTypes = new ArrayList<>();
  private final Set<String> usedNames = new HashSet<>();
  private final SqrlSchema schema;
  private final boolean addArguments;
  private final LogManager logManager;
  // Root path signifying the 'Query' type.
  private Map<NamePath, List<SqrlTableMacro>> objectPathToTables;
  // The path from root
  private Map<NamePath, List<SqrlTableMacro>> fieldPathToTables;
  public final Set<String> seen = new HashSet<>();

  @Inject
  public GraphqlSchemaFactory(SqrlSchema schema, CompilerConfig config, LogManager logManager) {
    this(schema, config.isAddArguments(), logManager);
  }

  public GraphqlSchemaFactory(SqrlSchema schema, boolean addArguments, LogManager logManager) {
    this.schema = schema;
    this.addArguments = addArguments;
    this.logManager = logManager;
  }

  public GraphQLSchema generate(ExecutionGoal goal) {
    this.objectPathToTables = schema.getTableFunctions().stream()
        .collect(Collectors.groupingBy(e -> e.getFullPath().popLast(),
            LinkedHashMap::new, Collectors.toList()));
    this.fieldPathToTables = schema.getTableFunctions().stream()
        .collect(Collectors.groupingBy(SqrlTableMacro::getAbsolutePath,
            LinkedHashMap::new, Collectors.toList()));
    for (Map.Entry<NamePath, List<SqrlTableMacro>> path : fieldPathToTables.entrySet()) {
      if (path.getKey().getLast().isHidden()) continue;

      if (goal == ExecutionGoal.TEST) {
        if (!path.getValue().get(0).isTest()) continue;
      } else {
        if (path.getValue().get(0).isTest()) continue;
      }
      Optional<GraphQLObjectType> graphQLObjectType = generateObject(path.getValue(),
          objectPathToTables.getOrDefault(path.getKey(), List.of()));
      graphQLObjectType.map(objectTypes::add);
    }

    GraphQLObjectType queryType = createQueryType(goal, objectPathToTables.get(NamePath.ROOT));

    postProcess();

    if (queryFields.isEmpty()) {
       return null;
    }


    GraphQLSchema.Builder builder = GraphQLSchema.newSchema()
        .query(queryType);
    if (goal != ExecutionGoal.TEST) {
      Optional<GraphQLObjectType.Builder> subscriptions = createSubscriptionTypes(schema);
      Optional<GraphQLObjectType.Builder> mutations = createMutationTypes(schema);
//      subscriptions.map(s->builder.subscription(s));
//      mutations.map(s->builder.mutation(s));
    }
    builder.additionalTypes(new LinkedHashSet<>(objectTypes));

    if (queryType.getFields().isEmpty()) {
      if (goal == ExecutionGoal.TEST) {
        throw new RuntimeException("No queries found for test goal");
      } else {
        throw new RuntimeException("No queryable tables found for server");
      }
    }

    //todo: hack because we can't merge scalars with graphql-java
    if (goal != ExecutionGoal.TEST) {
      builder
        .additionalType(CustomScalars.DATETIME);
    }

    return builder.build();
  }

  private Optional<Builder> createMutationTypes(SqrlSchema schema) {
    Builder builder = GraphQLObjectType.newObject().name("Mutation");

    if (schema.getImports().isEmpty()) {
      return Optional.empty();
    }

    for(ResolvedImport mutation : schema.getImports()) {
      String name = mutation.getName();
      if (name.startsWith(HIDDEN_PREFIX)) continue;

      RelDataType type = mutation.getRelDataType();

      // Create the 'event' argument which should mirror the structure of the type
      GraphQLInputType inputType = GraphqlSchemaUtil.createInputTypeForRelDataType(type, NamePath.of(name), seen).orElseThrow(
          () -> new IllegalArgumentException("Could not create input type for mutation: " + mutation.getName()));

      GraphQLArgument inputArgument = GraphQLArgument.newArgument()
          .name("event")
          .type(inputType)
          .build();

      GraphQLFieldDefinition subscriptionField = GraphQLFieldDefinition.newFieldDefinition()
          .name(name)
          .argument(inputArgument)
          .type(createOutputTypeForRelDataType(type, NamePath.of(name), seen).get())
          .build();

      builder.field(subscriptionField);
    }
    return Optional.of(builder);
  }

  public Optional<Builder> createSubscriptionTypes(SqrlSchema schema) {
    Builder subscriptionBuilder = GraphQLObjectType.newObject().name("Subscription");

    // Retrieve streamable tables from the schema
    List<PhysicalRelationalTable> streamTables = schema.getTableFunctions().stream()
        .filter(t-> t instanceof RootSqrlTable)
        .map(t->((PhysicalRelationalTable)((RootSqrlTable) t).getInternalTable()))
        .filter(t->!(t instanceof ProxyImportRelationalTable)) //do not create subscriptions for imported tables
        .filter(t-> t.getType() == TableType.STREAM)
        .collect(Collectors.toList());
    if (streamTables.isEmpty()) {
      return Optional.empty();
    }

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
          .type(createOutputTypeForRelDataType(table.getRowType(), NamePath.of(tableName), seen).get())
          .build();

      subscriptionBuilder.field(subscriptionField);
    }

    return Optional.of(subscriptionBuilder);
  }


  private GraphQLObjectType createQueryType(ExecutionGoal goal, List<SqrlTableMacro> relationships) {

    List<GraphQLFieldDefinition> fields = new ArrayList<>();

    for (SqrlTableMacro rel : relationships) {
      String name = rel.getAbsolutePath().getDisplay();
      if (name.startsWith(HIDDEN_PREFIX) || !isValidGraphQLName(name)) continue;

      if (goal == ExecutionGoal.TEST) {
        if (!rel.isTest()) continue;
      } else {
        if (rel.isTest()) continue;
      }

      GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
          .name(rel.getAbsolutePath().getDisplay())
          .type(wrap(createTypeName(rel), rel.getMultiplicity()))
          .arguments(createArguments(rel))
          .build();
      fields.add(field);
    }

    GraphQLObjectType objectType = GraphQLObjectType.newObject()
        .name("Query")
        .fields(fields)
        .build();

    usedNames.add("Query");

    this.queryFields.addAll(fields);

    return objectType;
  }

  private Optional<GraphQLObjectType> generateObject(List<SqrlTableMacro> tableMacros, List<SqrlTableMacro> relationships) {
    //Todo check: The multiple table macros should all point to the same relnode type

    Map<Name, List<SqrlTableMacro>> relByName = relationships.stream()
        .collect(Collectors.groupingBy(g -> g.getFullPath().getLast(),
            LinkedHashMap::new, Collectors.toList()));

    SqrlTableMacro first = tableMacros.get(0);
    RelDataType rowType = first.getRowType();
    //todo: check that all table macros are compatible
//    for (int i = 1; i < tableMacros.size(); i++) {
//    }

    List<GraphQLFieldDefinition> fields = new ArrayList<>();
    for (RelDataTypeField field : rowType.getFieldList()) {
      if (!relByName.containsKey(Name.system(field.getName()))) {
        createRelationshipField(field).map(fields::add);
      }
    }

    for (Map.Entry<Name, List<SqrlTableMacro>> rel : relByName.entrySet()) {
      createRelationshipField(rel.getValue()).map(fields::add);
    }

    if (fields.isEmpty()) {
      return Optional.empty();
    }

    String name = generateObjectName(first.getAbsolutePath());
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

  private Optional<GraphQLFieldDefinition> createRelationshipField(List<SqrlTableMacro> value) {
    SqrlTableMacro sqrlTableMacro = value.get(0);
    String name = sqrlTableMacro.getFullPath().getLast().getDisplay();
    if (!isValidGraphQLName(name) || isHiddenString(name)) {
      return Optional.empty();
    }

    GraphQLFieldDefinition field = GraphQLFieldDefinition.newFieldDefinition()
        .name(name)
        .type(wrap(createTypeName(sqrlTableMacro), sqrlTableMacro.getMultiplicity()))
        .arguments(createArguments(sqrlTableMacro))
        .build();

    return Optional.of(field);
  }

  private List<GraphQLArgument> createArguments(SqrlTableMacro field) {
    if (!allowedArguments(field)) {
      return List.of();
    }

    List<FunctionParameter> parameters = field.getParameters().stream()
        .filter(f->!((SqrlFunctionParameter)f).isInternal())
        .collect(Collectors.toList());
    if (addArguments && parameters.isEmpty() && field.getJoinType() == JoinType.JOIN) {
      List<GraphQLArgument> limitOffset = generateLimitOffset();
      return limitOffset;
    } else if (addArguments && parameters.isEmpty()) {
      NamePath toTable = schema.getPathToAbsolutePathMap()
          .get(field.getFullPath());
      List<SqrlTableMacro> sqrlTableMacros = fieldPathToTables.get(toTable);
      List<GraphQLArgument> premuted = generatePermuted(sqrlTableMacros.get(0));
      List<GraphQLArgument> limitOffset = generateLimitOffset();

      return ListUtils.union(premuted, limitOffset);
    } else {
      return parameters.stream()
          .filter(p->!((SqrlFunctionParameter)p).isInternal())
          .filter(p->getInputType(p.getType(null), NamePath.of(p.getName()), seen).isPresent())
          .map(parameter -> GraphQLArgument.newArgument()
              .name(((SqrlFunctionParameter)parameter).getVariableName())
              .type(nonNull(getInputType(parameter.getType(null), NamePath.of(parameter.getName()), seen).get()))
              .build()).collect(Collectors.toList());
    }
  }

  private boolean allowedArguments(SqrlTableMacro field) {
    //No arguments for to-one rels or parent fields
    return field.getMultiplicity().equals(Multiplicity.MANY) &&
        !field.getJoinType().equals(JoinType.PARENT);
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
        .filter(f -> getInputType(f.getType(), NamePath.of(tableName), seen).isPresent())
        .filter(f -> isValidGraphQLName(f.getName()))
        .filter(this::isVisible)
        .map(f -> GraphQLArgument.newArgument()
            .name(f.getName())
            .type(getInputType(f.getType(), NamePath.of(f.getName()), seen).get())
            .build())
        .collect(Collectors.toList());
  }

  private boolean isVisible(RelDataTypeField f) {
    return !f.getName().startsWith(HIDDEN_PREFIX);
  }

  private GraphQLOutputType createTypeName(SqrlTableMacro sqrlTableMacro) {
    seen.add(sqrlTableMacro.getAbsolutePath().getLast().getDisplay());

    return new GraphQLTypeReference(sqrlTableMacro.getAbsolutePath().getLast().getDisplay());
  }

  private Optional<GraphQLFieldDefinition> createRelationshipField(RelDataTypeField field) {
    return getOutputType(field.getType(), NamePath.of(field.getName()), seen)
        .filter(f->isValidGraphQLName(field.getName()))
        .filter(f->isVisible(field))
        .map(t -> GraphQLFieldDefinition.newFieldDefinition()
            .name(field.getName())
            .type(wrap(t, field.getType())).build());
  }

  public void postProcess() {
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