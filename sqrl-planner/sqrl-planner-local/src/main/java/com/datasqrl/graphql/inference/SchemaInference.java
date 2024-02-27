///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl.graphql.inference;
//
//import static com.datasqrl.graphql.inference.SchemaBuilder.generateCombinations;
//import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;
//import static com.datasqrl.graphql.util.GraphqlCheckUtil.createThrowable;
//import static com.datasqrl.graphql.server.BuildGraphQLEngine.DUMMY_QUERY;
//import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getMutationTypeName;
//import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getQueryTypeName;
//import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getSubscriptionTypeName;
//
//import com.datasqrl.calcite.SqrlFramework;
//import com.datasqrl.calcite.function.SqrlTableMacro;
//import com.datasqrl.canonicalizer.Name;
//import com.datasqrl.canonicalizer.ReservedName;
//import com.datasqrl.config.SerializedSqrlConfig;
//import com.datasqrl.graphql.APIConnectorManager;
//import com.datasqrl.graphql.inference.SchemaBuilder.ArgCombination;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredField;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredMutation;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredMutations;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredObjectField;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredQuery;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredScalarField;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSchema;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscription;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.InferredSubscriptions;
//import com.datasqrl.graphql.inference.SchemaInferenceModel.NestedField;
//import com.datasqrl.graphql.inference.SqrlSchemaForInference.Column;
//import com.datasqrl.graphql.inference.SqrlSchemaForInference.Field;
//import com.datasqrl.graphql.inference.SqrlSchemaForInference.Relationship;
//import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable;
//import com.datasqrl.graphql.server.Model;
//import com.datasqrl.graphql.server.Model.Argument;
//import com.datasqrl.graphql.server.Model.RootGraphqlModel;
//import com.datasqrl.graphql.server.Model.StringSchema;
//import com.datasqrl.io.tables.TableSink;
//import com.datasqrl.loaders.ModuleLoader;
//import com.datasqrl.plan.queries.APISource;
//import com.datasqrl.util.SqlNameUtil;
//import graphql.language.EnumTypeDefinition;
//import graphql.language.FieldDefinition;
//import graphql.language.ImplementingTypeDefinition;
//import graphql.language.InputObjectTypeDefinition;
//import graphql.language.InputValueDefinition;
//import graphql.language.ListType;
//import graphql.language.NamedNode;
//import graphql.language.NonNullType;
//import graphql.language.ObjectTypeDefinition;
//import graphql.language.ScalarTypeDefinition;
//import graphql.language.Type;
//import graphql.language.TypeDefinition;
//import graphql.language.TypeName;
//import graphql.schema.idl.SchemaParser;
//import graphql.schema.idl.TypeDefinitionRegistry;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.HashSet;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Set;
//import java.util.stream.Collectors;
//import lombok.Getter;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.calcite.schema.Function;
//import org.apache.calcite.tools.RelBuilder;
//
//@Getter
//@Slf4j
///**
// * Walk the sqrl schema
// * 1. If no tblfn can be found, log as unknown query but don't fail
// *
// * How do we walk the schema?
// * Options: table function centric, schema centric
// * Table functions: easy to iterate over, no graph path stuff, needs to check at end for sat,
// *   How to resolve fields? ( in object centric view ), how to resolve mutations/subscriptions?
// * gql centric: harder to iterate over, sat check during walking,
// *
// * Separate walking from action
// * 1. visit(parent, ObjectType, SqrlTableMacro[]) (all permutations then its easy to err?)
// *
// * 1. graphql visitor then a strategy?
// * visitQuery
// * visitMutation
// * visitSubscription
// * visitObject
// * visitEnum
// * visitUnion
// *
// *
// */
//public class SchemaInference {
//
//  private final SqrlFramework framework;
//  private final String name;
//  private final ModuleLoader moduleLoader;
//  private final APISource source;
//  private final TypeDefinitionRegistry registry;
//  private final SqrlSchemaForInference schema;
//  private final RootGraphqlModel.RootGraphqlModelBuilder root;
//  private final RelBuilder relBuilder;
//  private final APIConnectorManager apiManager;
//  private final Set<FieldDefinition> visited = new HashSet<>();
//  private final Map<ObjectTypeDefinition, SQRLTable> visitedObj = new HashMap<>();
//  private final GraphqlQueryBuilder graphqlQueryBuilder;
//
//  public SchemaInference(SqrlFramework framework, String name, ModuleLoader moduleLoader, APISource apiSchema, SqrlSchemaForInference schema,
//                         RelBuilder relBuilder, APIConnectorManager apiManager) {
//    this.framework = framework;
//    this.name = name;
//    this.moduleLoader = moduleLoader;
//    this.source = apiSchema;
//    this.registry = (new SchemaParser()).parse(apiSchema.getSchemaDefinition());
//    this.schema = schema;
//    this.root = RootGraphqlModel.builder()
//        .schema(StringSchema.builder().schema(apiSchema.getSchemaDefinition()).build());
//    this.relBuilder = relBuilder;
//    this.apiManager = apiManager;
//    this.graphqlQueryBuilder  = new GraphqlQueryBuilder(framework, apiManager,
//        new SqlNameUtil(framework.getNameCanonicalizer()));
//  }
//
//  public InferredSchema accept() {
//    //resolve additional types
//    resolveTypes();
//
//    InferredQuery query = registry.getType(getQueryTypeName(registry))
//        .map(q -> resolveQueries((ObjectTypeDefinition) q))
//        .orElseGet(this::createDummyQuery);
//
//    Optional<InferredMutations> mutation = registry.getType(getMutationTypeName(registry))
//        .map(m -> resolveMutations((ObjectTypeDefinition) m));
//
//    Optional<InferredSubscriptions> subscription = registry.getType(getSubscriptionTypeName(registry))
//        .map(s -> resolveSubscriptions((ObjectTypeDefinition) s));
//
//    return new InferredSchema(name, query, mutation, subscription);
//  }
//
//  private InferredQuery createDummyQuery() {
//    return new InferredQuery(DUMMY_QUERY,
//        List.of());
//  }
//
//  private void resolveTypes() {
//    //todo custom types: walk all defined types and import them
//  }
//
//  private InferredQuery resolveQueries(ObjectTypeDefinition query) {
//    List<InferredField> fields = new ArrayList<>();
//    for (FieldDefinition fieldDefinition : query.getFieldDefinitions()) {
//      visited.add(fieldDefinition);
//      fields.add(resolveQueryFromSchema(fieldDefinition, fields, query));
//    }
//
//    return new InferredQuery(query, fields);
//  }
//
//  private InferredField resolveQueryFromSchema(FieldDefinition fieldDefinition,
//      List<InferredField> fields, ObjectTypeDefinition parentDefinition) {
//    Optional<Function> function = framework.getSchema().getFunctions(fieldDefinition.getName(), false)
//        .stream()
//        .findFirst();
//
//    checkState(function.isPresent(), fieldDefinition.getSourceLocation(),
//        "Could not find Query function %s", fieldDefinition.getName());
//
//    SQRLTable table = resolveRootSQRLTable(fieldDefinition, fieldDefinition.getName());
//    return inferObjectField(fieldDefinition, table, fields, parentDefinition, null, null,
//        createQueries(parentDefinition, fieldDefinition, null, (SqrlTableMacro) function.get()));
//  }
//
//  private List<Model.ArgumentSet> createQueries(ObjectTypeDefinition parentDefinition,
//      FieldDefinition fieldDefinition, SQRLTable fromTable, SqrlTableMacro macro) {
//
//    List<Model.ArgumentSet> arguments = new ArrayList<>();
//
//    List<List<ArgCombination>> argCombinations = generateCombinations(
//        fieldDefinition.getInputValueDefinitions());
//
//    for (List<ArgCombination> arg : argCombinations) {
////      Model.ArgumentSet query = graphqlQueryBuilder.create(arg, macro, parentDefinition.getName(), fieldDefinition,
////          fromTable == null ? null : fromTable.getRelOptTable().getRowType(null));
////      arguments.add(query);
//    }
//
//    // Verify we created the correct argument sets
//    Set<Set<Argument>> seen = new HashSet<>();
//    for (Model.ArgumentSet argSet : arguments) {
//      checkState(!seen.contains(argSet.getArguments()), fieldDefinition.getSourceLocation(),
//          "Duplicate argument set (System error)");
//      seen.add(argSet.getArguments());
//    }
//
//    return arguments;
//  }
//
//  private SQRLTable resolveRootSQRLTable(FieldDefinition fieldDefinition, String fieldName) {
//    SQRLTable sqrlTable = schema.getRootSqrlTable(fieldName);
//    checkState(sqrlTable != null, fieldDefinition.getSourceLocation(), "Could not find associated SQRL table for field %s on type %s.",
//        fieldDefinition.getName(), fieldDefinition.getType() instanceof TypeName
//            ? ((TypeName) fieldDefinition.getType()).getName()
//            : unwrapObjectType(fieldDefinition.getType()).getName());
//
//    return sqrlTable;
//  }
//
//  private InferredField inferObjectField(FieldDefinition fieldDefinition, SQRLTable sqrlTable,
//      List<InferredField> fields, ObjectTypeDefinition parentDefinition, SQRLTable parentSqrlTable,
//      SqrlTableMacro tableMacro, List<Model.ArgumentSet> argumentSets) {
//    checkValidArrayNonNullType(fieldDefinition.getType());
//    TypeDefinition fieldDefinitionType = unwrapObjectType(fieldDefinition.getType());
//
//    checkState(fieldDefinitionType instanceof ObjectTypeDefinition, fieldDefinitionType.getSourceLocation(),
//        "Could not infer non-object type on graphql schema: %s", fieldDefinition.getName());
//
//    //one deep check
//    ObjectTypeDefinition objectDefinition = (ObjectTypeDefinition) fieldDefinitionType;
//    if (visitedObj.get(objectDefinition) != null && !visitedObj.get(objectDefinition).getIsTypeOf().isEmpty()) {
//      if (!sqrlTable.getIsTypeOf().contains(visitedObj.get(objectDefinition).getIsTypeOf().get(0)))
//        checkState(visitedObj.get(objectDefinition) == null || visitedObj.get(objectDefinition) == sqrlTable,
//            fieldDefinitionType.getSourceLocation(),
//            "Cannot redefine a type to point to a different SQRL table. Use an interface instead.\n"
//                + "The graphql field [%s] points to Sqrl table [%s] but already had [%s].",
//            parentDefinition.getName() + ":" + fieldDefinition.getName(),
//            sqrlTable.getPath().toString(), "");
//    }
//    visitedObj.put(objectDefinition, sqrlTable);
//    InferredObjectField inferredObjectField = new InferredObjectField(parentSqrlTable, parentDefinition, fieldDefinition,
//        (ObjectTypeDefinition) fieldDefinitionType, sqrlTable, tableMacro, argumentSets);
//    fields.addAll(walkChildren((ObjectTypeDefinition) fieldDefinitionType, sqrlTable, fields));
//    return inferredObjectField;
//  }
//
//  private List<InferredField> walkChildren(ObjectTypeDefinition typeDef, SQRLTable table,
//      List<InferredField> fields) {
//    List<FieldDefinition> invalidFields = getInvalidFields(typeDef, table);
//    boolean structurallyEqual = structurallyEqual(typeDef, table);
//    //todo clean up, add lazy evaluation
//    checkState(structurallyEqual,
//        invalidFields.isEmpty() ? typeDef.getSourceLocation() : invalidFields.get(invalidFields.size()-1).getSourceLocation(),
//        "Field(s) [%s] could not be found on type [%s]. Possible fields are: [%s]",
//        String.join(",", invalidFields.stream()
//            .map(FieldDefinition::getName)
//            .collect(Collectors.toList())),
//        typeDef.getName(),
//        String.join(", ", table.tableMacro.getRowType().getFieldNames()));
//
//    return typeDef.getFieldDefinitions().stream()
//        .filter(f -> !visited.contains(f))
//        .map(f -> walk(f, table.getField(Name.system(f.getName())).get(), fields, typeDef))
//        .collect(Collectors.toList());
//  }
//
//  private InferredField walk(FieldDefinition fieldDefinition, Field field,
//      List<InferredField> fields, ObjectTypeDefinition parent) {
//    visited.add(fieldDefinition);
//    if (field instanceof Relationship) {
//      return walkRel(fieldDefinition, (Relationship) field, fields, parent);
//    } else {
//      return walkScalar(fieldDefinition, (Column) field, parent);
//    }
//  }
//
//  private InferredField walkRel(FieldDefinition fieldDefinition, Relationship relationship,
//      List<InferredField> fields, ObjectTypeDefinition parent) {
//
//    return new NestedField(relationship,
//        inferObjectField(fieldDefinition, relationship.getToTable(),
//            fields, parent, relationship.getFromTable(), relationship.macro,
//            createQueries(parent, fieldDefinition, relationship.fromTable,
//                relationship.getMacro())));
//  }
//
//  private InferredField walkScalar(FieldDefinition fieldDefinition, Column column,
//      ObjectTypeDefinition parent) {
//    checkValidArrayNonNullType(fieldDefinition.getType());
//
//    TypeDefinition type = unwrapObjectType(fieldDefinition.getType());
//    //Todo: expand server to allow type coercion
//    //Todo: enums
//    checkState(type instanceof ScalarTypeDefinition
//            || type instanceof EnumTypeDefinition, type.getSourceLocation(),
//        "Unknown type found: %s", type.getName());
//
//    return new InferredScalarField(fieldDefinition, column, parent);
//  }
//
//
//  private void checkValidArrayNonNullType(Type type) {
//    Type root = type;
//    if (type instanceof NonNullType) {
//      type = ((NonNullType) type).getType();
//    }
//    if (type instanceof ListType) {
//      type = ((ListType) type).getType();
//    }
//    if (type instanceof NonNullType) {
//      type = ((NonNullType) type).getType();
//    }
//    checkState(type instanceof TypeName, root.getSourceLocation(),
//        "Type must be a non-null array, array, or non-null");
//  }
//
//  private boolean structurallyEqual(ImplementingTypeDefinition typeDef, SQRLTable table) {
//    return typeDef.getFieldDefinitions().stream()
//        .allMatch(f -> table.getField(Name.system(((NamedNode)f).getName())).isPresent());
//  }
//
//  private List<FieldDefinition> getInvalidFields(ObjectTypeDefinition typeDef, SQRLTable table) {
//    return typeDef.getFieldDefinitions().stream()
//        .filter(f -> table.getField(Name.system(f.getName())).isEmpty())
//        .collect(Collectors.toList());
//  }
//
//  private TypeDefinition unwrapObjectType(Type type) {
//    //type can be in a single array with any non-nulls, e.g. [customer!]!
//    type = unboxNonNull(type);
//    if (type instanceof ListType) {
//      type = ((ListType) type).getType();
//    }
//    type = unboxNonNull(type);
//
//    Optional<TypeDefinition> typeDef = this.registry.getType(type);
//
//    checkState(typeDef.isPresent(), type.getSourceLocation(),
//        "Could not find Object type [%s]", type instanceof TypeName ?
//            ((TypeName) type).getName() : type.toString());
//
//    return typeDef.get();
//  }
//
//  private Type unboxNonNull(Type type) {
//    if (type instanceof NonNullType) {
//      return unboxNonNull(((NonNullType) type).getType());
//    }
//    return type;
//  }
//
//  private InferredMutations resolveMutations(ObjectTypeDefinition mutation) {
//    List<InferredMutation> mutations = new ArrayList<>();
//    for(FieldDefinition fieldDefinition : mutation.getFieldDefinitions()) {
//      validateStructurallyEqualMutation(fieldDefinition, getValidMutationReturnType(fieldDefinition), getValidMutationInput(fieldDefinition),
//          List.of(ReservedName.SOURCE_TIME.getCanonical()));
//      TableSink tableSink = apiManager.getMutationSource(source, Name.system(fieldDefinition.getName()));
//      checkState(tableSink != null, mutation.getSourceLocation(),
//          "Could not find mutation source: %s.", fieldDefinition.getName());
//
//      //TODO: validate that tableSink schema matches Input type
//      SerializedSqrlConfig config = tableSink.getConfiguration().getConfig().serialize();
//      InferredMutation inferredMutation = new InferredMutation(fieldDefinition.getName(), config);
//      mutations.add(inferredMutation);
//    }
//
//    return new InferredMutations(mutations);
//  }
//
//  private Object validateStructurallyEqualMutation(FieldDefinition fieldDefinition,
//      ObjectTypeDefinition returnTypeDefinition, InputObjectTypeDefinition inputType,
//      List<String> allowedFieldNames) {
//
//    //The return type can have _source_time
//    for (FieldDefinition returnTypeFieldDefinition : returnTypeDefinition.getFieldDefinitions()) {
//      if (allowedFieldNames.contains(returnTypeFieldDefinition.getName())) {
//        continue;
//      }
//
//      String name = returnTypeFieldDefinition.getName();
//      InputValueDefinition inputDefinition = findExactlyOneInputValue(fieldDefinition, name,
//          inputType.getInputValueDefinitions());
//
//      //validate type structurally equal
//      validateStructurallyEqualMutation(returnTypeFieldDefinition, inputDefinition);
//    }
//
//    return null;
//  }
//
//  private void validateStructurallyEqualMutation(FieldDefinition fieldDefinition,
//      InputValueDefinition inputDefinition) {
//    checkState(fieldDefinition.getName().equals(inputDefinition.getName()), fieldDefinition.getSourceLocation(),
//        "Name must be equal to the input name {} {}", fieldDefinition.getName(), inputDefinition.getName());
//    Type definitionType = fieldDefinition.getType();
//    Type inputType = inputDefinition.getType();
//
//    validateStructurallyType(fieldDefinition, definitionType, inputType);
//  }
//
//  private Object validateStructurallyType(FieldDefinition field, Type definitionType, Type inputType) {
//    if (inputType instanceof NonNullType) {
//      //subType may be nullable if type is non-null
//      NonNullType nonNullType = (NonNullType) inputType;
//      if (definitionType instanceof NonNullType) {
//        NonNullType nonNullDefinitionType = (NonNullType) definitionType;
//        return validateStructurallyType(field, nonNullDefinitionType.getType(), nonNullType.getType());
//      } else {
//        return validateStructurallyType(field, definitionType, nonNullType.getType());
//      }
//    } else if (inputType instanceof ListType) {
//      //subType must be a list
//      checkState(definitionType instanceof ListType, definitionType.getSourceLocation(),
//          "List type mismatch for field. Must match the input type. " + field.getName());
//      ListType inputListType = (ListType) inputType;
//      ListType definitionListType = (ListType) definitionType;
//      return validateStructurallyType(field, definitionListType.getType(), inputListType.getType());
//    } else if (inputType instanceof TypeName) {
//      //If subtype nonnull then it could return errors
//      checkState(!(definitionType instanceof NonNullType), definitionType.getSourceLocation(),
//          "Non-null found on field %s, could result in errors if input type is null",
//          field.getName());
//      checkState(!(definitionType instanceof ListType), definitionType.getSourceLocation(),
//          "List type found on field %s when the input is a scalar type",
//          field.getName());
//
//      //If typeName, resolve then
//      TypeName inputTypeName = (TypeName) inputType;
//      TypeName defTypeName = (TypeName) definitionType;
//      TypeDefinition inputTypeDef = registry.getType(inputTypeName)
//          .orElseThrow(()->createThrowable(inputTypeName.getSourceLocation(),
//              "Could not find type: %s", inputTypeName.getName()));
//      TypeDefinition defTypeDef = registry.getType(defTypeName)
//          .orElseThrow(()->createThrowable(defTypeName.getSourceLocation(),
//              "Could not find type: %s", defTypeName.getName()));
//
//      //If input or scalar
//      if (inputTypeDef instanceof ScalarTypeDefinition) {
//        checkState(defTypeDef instanceof ScalarTypeDefinition &&
//            inputTypeDef.getName().equals(defTypeDef.getName()), field.getSourceLocation(),
//            "Scalar types not matching for field [%s]: found %s but wanted %s", field.getName(),
//            inputTypeDef.getName(), defTypeDef.getName());
//        return null;
//      } else if (inputTypeDef instanceof EnumTypeDefinition) {
//        checkState(defTypeDef instanceof EnumTypeDefinition || defTypeDef instanceof ScalarTypeDefinition &&
//                inputTypeDef.getName().equals(defTypeDef.getName()), field.getSourceLocation(),
//            "Enum types not matching for field [%s]: found %s but wanted %s", field.getName(),
//            inputTypeDef.getName(), defTypeDef.getName());
//        return null;
//      } else if (inputTypeDef instanceof InputObjectTypeDefinition){
//        checkState(defTypeDef instanceof ObjectTypeDefinition, field.getSourceLocation(),
//            "Return object type must match with an input object type not matching for field [%s]: found %s but wanted %s", field.getName(),
//            inputTypeDef.getName(), defTypeDef.getName());
//        ObjectTypeDefinition objectDefinition = (ObjectTypeDefinition) defTypeDef;
//        InputObjectTypeDefinition inputDefinition = (InputObjectTypeDefinition) inputTypeDef;
//        return validateStructurallyEqualMutation(field, objectDefinition, inputDefinition, List.of());
//      } else {
//        throw createThrowable(inputTypeDef.getSourceLocation(),
//            "Unknown type encountered: %s", inputTypeDef.getName());
//      }
//    }
//
//    throw createThrowable(field.getSourceLocation(),
//        "Unknown type encountered for field: %s", field.getName());
//  }
//
//  private InputValueDefinition findExactlyOneInputValue(FieldDefinition fieldDefinition, String name,
//      List<InputValueDefinition> inputValueDefinitions) {
//    InputValueDefinition found = null;
//    for (InputValueDefinition inputDefinition : inputValueDefinitions) {
//      if (inputDefinition.getName().equals(name)) {
//        checkState(found == null, inputDefinition.getSourceLocation(),
//            "Duplicate fields found");
//        found = inputDefinition;
//      }
//    }
//
//    checkState(found != null, fieldDefinition.getSourceLocation(),
//        "Could not find field %s in type %s", name, fieldDefinition.getName());
//
//    return found;
//  }
//
//  private ObjectTypeDefinition getValidMutationReturnType(FieldDefinition fieldDefinition) {
//    Type type = fieldDefinition.getType();
//    if (type instanceof NonNullType) {
//      type = ((NonNullType)type).getType();
//    }
//
//    checkState(type instanceof TypeName, type.getSourceLocation(),
//        "[%s] must be a singular return value", fieldDefinition.getName());
//    TypeName name = (TypeName) type;
//
//    TypeDefinition typeDef = registry.getType(name)
//        .orElseThrow(()->createThrowable(name.getSourceLocation(),
//            "Could not find return type: %s"));
//    checkState(typeDef instanceof ObjectTypeDefinition, typeDef.getSourceLocation(),
//        "Return must be an object type: %s", fieldDefinition.getName());
//
//    return (ObjectTypeDefinition) typeDef;
//  }
//
//  private InputObjectTypeDefinition getValidMutationInput(FieldDefinition fieldDefinition) {
//    checkState(!(fieldDefinition.getInputValueDefinitions().isEmpty()), fieldDefinition.getSourceLocation(), fieldDefinition.getName() + " has too few arguments. Must have one non-null input type argument.");
//    checkState(fieldDefinition.getInputValueDefinitions().size() == 1, fieldDefinition.getSourceLocation(), fieldDefinition.getName() + " has too many arguments. Must have one non-null input type argument.");
//    checkState(fieldDefinition.getInputValueDefinitions().get(0).getType() instanceof NonNullType, fieldDefinition.getSourceLocation(),
//        "[" + fieldDefinition.getName() + "] " + fieldDefinition.getInputValueDefinitions().get(0).getName()
//              + "Must be non-null.");
//    NonNullType nonNullType = (NonNullType)fieldDefinition.getInputValueDefinitions().get(0).getType();
//    checkState(nonNullType.getType() instanceof TypeName, fieldDefinition.getSourceLocation(), "Must be a singular value");
//    TypeName name = (TypeName) nonNullType.getType();
//
//    Optional<TypeDefinition> typeDef = registry.getType(name);
//    checkState(typeDef.isPresent(), fieldDefinition.getSourceLocation(), "Could not find input type:" + name.getName());
//    checkState(typeDef.get() instanceof InputObjectTypeDefinition, fieldDefinition.getSourceLocation(), "Input must be an input object type:" + fieldDefinition.getName());
//
//    return (InputObjectTypeDefinition) typeDef.get();
//  }
//
//  private InferredSubscriptions resolveSubscriptions(ObjectTypeDefinition subscriptionDefinition) {
//    List<InferredSubscription> subscriptions = new ArrayList<>();
//    //todo validation and nested queries
//    List<InferredField> fields = new ArrayList<>();
//
//    for(FieldDefinition subscriptionField : subscriptionDefinition.getFieldDefinitions()) {
//      SQRLTable table = resolveRootSQRLTable(subscriptionField, subscriptionField.getName());
//      //Resolve the queries for all nested entries, validate all fields are sqrl fields
//      TypeDefinition returnType = registry.getType(subscriptionField.getType())
//          .orElseThrow(()->createThrowable(subscriptionField.getSourceLocation(),
//              "Could not find subscription type"));
//      //todo check before cast
//      ObjectTypeDefinition objectTypeDefinition = (ObjectTypeDefinition) returnType;
//
//      for (FieldDefinition definition : objectTypeDefinition.getFieldDefinitions()) {
//        Field field = table.getField(Name.system(definition.getName()))
//            .orElseThrow(() -> createThrowable(definition.getSourceLocation(),
//                "Unrecognized field %s in %s", definition.getName(),
//                objectTypeDefinition.getName()));
//        InferredField inferredField = walk(definition, field, fields, objectTypeDefinition);
//        fields.add(inferredField);
//      }
//
//      //user defined field to internal field map
//      Map<String, String> filters = new HashMap<>();
//      for (InputValueDefinition input : subscriptionField.getInputValueDefinitions()) {
//        Name fieldId = table.getField(Name.system(input.getName())).get().getName();
//        filters.put(input.getName(), fieldId.getDisplay());
//      }
//
//      /*
//       * Match parameters, error if there are any on the sqrl function
//       */
//      InferredSubscription inferredSubscription = new InferredSubscription(subscriptionField.getName(),
//          table, source, filters);
//      subscriptions.add(inferredSubscription);
//    }
//    return new InferredSubscriptions(subscriptions, fields);
//  }
//}