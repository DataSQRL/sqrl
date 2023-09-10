/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.inference;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.SqrlPreparingTable;
import com.datasqrl.calcite.SqrlRelBuilder;
import com.datasqrl.calcite.schema.SqrlTableFunction;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.config.SerializedSqrlConfig;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.error.ErrorLabel;
import com.datasqrl.graphql.generate.SchemaGeneratorUtil;
import com.datasqrl.graphql.inference.SchemaInferenceModel.*;
import com.datasqrl.graphql.inference.argument.ArgumentHandler;
import com.datasqrl.graphql.inference.argument.EqHandler;
import com.datasqrl.graphql.inference.argument.LimitOffsetHandler;
import com.datasqrl.graphql.server.Model.RootGraphqlModel;
import com.datasqrl.graphql.server.Model.StringSchema;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.plan.local.generate.TableFunctionBase;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.datasqrl.parse.SqrlAstException;
import com.datasqrl.plan.table.VirtualRelationalTable;
import com.datasqrl.schema.Column;
import com.datasqrl.schema.Field;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import graphql.language.EnumTypeDefinition;
import graphql.language.FieldDefinition;
import graphql.language.ImplementingTypeDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.NamedNode;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.SourceLocation;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.lang3.StringUtils;
import com.google.common.base.Preconditions;

@Getter
@Slf4j
public class SchemaInference {

  private final SqrlFramework framework;
  private final String name;
  private final ModuleLoader moduleLoader;
  private final APISource source;
  private final TypeDefinitionRegistry registry;
  private final SqrlSchema schema;
  private final RootGraphqlModel.RootGraphqlModelBuilder root;
  private final List<ArgumentHandler> argumentHandlers = List.of(new EqHandler(), new LimitOffsetHandler());
  private final RelBuilder relBuilder;
  private final APIConnectorManager apiManager;
  private Set<FieldDefinition> visited = new HashSet<>();
  private final Map<ObjectTypeDefinition, SQRLTable> visitedObj = new HashMap<>();

  public SchemaInference(SqrlFramework framework, String name, ModuleLoader moduleLoader, APISource apiSchema, SqrlSchema schema,
                         RelBuilder relBuilder, APIConnectorManager apiManager) {
    this.framework = framework;
    this.name = name;
    this.moduleLoader = moduleLoader;
    this.source = apiSchema;
    this.registry = (new SchemaParser()).parse(apiSchema.getSchemaDefinition());
    this.schema = schema;
    this.root = RootGraphqlModel.builder()
        .schema(StringSchema.builder().schema(apiSchema.getSchemaDefinition()).build());
    this.relBuilder = relBuilder;
    this.apiManager = apiManager;
  }

  public InferredSchema accept() {
    //resolve additional types
    resolveTypes();

    InferredQuery query = registry.getType("Query")
        .map(q -> resolveQueries((ObjectTypeDefinition) q))
        .orElseThrow(() -> new RuntimeException("Must have a query type"));

    Optional<InferredMutations> mutation = registry.getType("Mutation")
        .map(m -> resolveMutations((ObjectTypeDefinition) m));

    Optional<InferredSubscriptions> subscription = registry.getType("Subscription")
        .map(s -> resolveSubscriptions((ObjectTypeDefinition) s));

    InferredSchema inferredSchema = new InferredSchema(name, query, mutation, subscription);
    return inferredSchema;
  }

  private void resolveTypes() {
    //todo custom types: walk all defined types and import them
  }

  private InferredQuery resolveQueries(ObjectTypeDefinition query) {
    List<InferredField> fields = new ArrayList<>();
    for (FieldDefinition fieldDefinition : query.getFieldDefinitions()) {
      visited.add(fieldDefinition);
      fields.add(resolveQueryFromSchema(fieldDefinition, fields, query));
    }

    return new InferredQuery(query, fields);
  }

  private InferredField resolveQueryFromSchema(FieldDefinition fieldDefinition,
      List<InferredField> fields, ObjectTypeDefinition parent) {
    SQRLTable table = resolveRootSQRLTable(fieldDefinition, fieldDefinition.getType(), fieldDefinition.getName(), "Query");
    return inferObjectField(fieldDefinition, table, fields, parent, null);
  }

  private SQRLTable resolveRootSQRLTable(FieldDefinition fieldDefinition,
      Type fieldType, String fieldName, String rootType) {
    SqlUserDefinedTableFunction function = SqrlRelBuilder
        .getSqrlTableFunction(framework.getQueryPlanner(), List.of(fieldName));

    if (function == null) {
      Optional<SQRLTable> sqrlTableByFieldType = getTypeName(fieldType)
          .flatMap(name -> getTableOfType(fieldType, name));
      if (sqrlTableByFieldType.isPresent()) {
        return sqrlTableByFieldType.get();
      }

      throw new SqrlAstException(ErrorLabel.GENERIC,
          toParserPos(fieldDefinition.getSourceLocation()),
          "Could not find associated SQRL table for field %s on type %s.",
          fieldDefinition.getName(), fieldDefinition.getType() instanceof TypeName ?
          ((TypeName) fieldDefinition.getType()).getName() : fieldDefinition.getType().toString());
    }

    SqrlTableFunction tableFunction = (SqrlTableFunction)function.getFunction();

    //TODO: Validate all fields are there
    SqrlPreparingTable table = framework.getCatalogReader().getSqrlTable(List.of(fieldName));
    //wrong
    if (table != null) {
      return framework.getCatalogReader().getSqrlTable(List.of(fieldName))
          .unwrap(VirtualRelationalTable.class)
          .getSqrlTable();
    } else {
      System.out.println();
      return framework.getSchema().getRootTables().stream()
          .filter(f->f.getName().getCanonical().equals(fieldName.toLowerCase()))
          .findAny().get();
    }
  }

  private Optional<? extends SQRLTable> getTableOfCommonInterface(Type type, String name) {
    Optional<TypeDefinition> defOpt = registry.getType(type);
    if (defOpt.isPresent()) {
      TypeDefinition def = defOpt.get();
      //Get all extending types, if there is one unique one then return it
      if (def instanceof ObjectTypeDefinition) {
        ObjectTypeDefinition objDef = (ObjectTypeDefinition) def;
        List<SQRLTable> foundTables = objDef.getImplements()
            .stream()
            .map(t->getTableOfType(t, getTypeName(t).get()))
            .filter(o->o.isPresent())
            .map(f->f.get())
            .collect(Collectors.toList());
        if (foundTables.size() == 1) {
          return Optional.of(foundTables.get(0));
        }
      }
    }

    return Optional.empty();
  }

  private Optional<SQRLTable> getTableOfType(Type type, String name) {
    for (SQRLTable table : schema.getRootTables()) {
      if (isNameEqual(table.getName().getCanonical(), name) &&
          lookupType(type).filter(t -> structurallyEqual(t, table)).isPresent()) {
        return Optional.of(table);
      }
    }
    return Optional.empty();
  }

  private Optional<String> getTypeName(Type type) {
    return registry.getType(type)
        .map(f->f.getName());
  }

  private boolean isNameEqual(String canonical, String graphqlName) {
    String conform = SchemaGeneratorUtil.conformName(canonical);

    return conform.equalsIgnoreCase(Name.system(graphqlName).getCanonical()) ||
        conform.equalsIgnoreCase(Name.system(stripTrailing(graphqlName)).getCanonical());
  }

  //Graphql generator may add trailing _ for name collisions
  private String stripTrailing(String graphqlName) {
    return StringUtils.stripEnd(graphqlName, "_");
  }

  private Optional<ImplementingTypeDefinition> lookupType(Type type) {
    return registry.getType(type)
        .filter(i -> i instanceof ImplementingTypeDefinition)
        .map(o -> (ImplementingTypeDefinition) o);
  }

  private InferredField inferObjectField(FieldDefinition fieldDefinition, SQRLTable table,
      List<InferredField> fields, ObjectTypeDefinition parent, SQRLTable parentTable) {
    checkValidArrayNonNullType(fieldDefinition.getType());
    TypeDefinition typeDef = unwrapObjectType(fieldDefinition.getType());

    if (typeDef instanceof ObjectTypeDefinition) {
      ObjectTypeDefinition obj = (ObjectTypeDefinition) typeDef;
      checkState(visitedObj.get(obj) == null || visitedObj.get(obj) == table, typeDef.getSourceLocation(),
          "Cannot redefine a type to point to a different SQRL table. Use an interface instead.\n"
              + "Table [%s] points to a different SQRL table.",
          table.getName().getDisplay());
      visitedObj.put(obj, table);
      InferredObjectField inferredObjectField = new InferredObjectField(parentTable, parent, fieldDefinition,
          (ObjectTypeDefinition) typeDef, table);
      fields.addAll(walkChildren((ObjectTypeDefinition) typeDef, table, fields));
      return inferredObjectField;
    } else {
      throw new RuntimeException("Could not infer non-object type on graphql schema: " + fieldDefinition.getName());
    }
  }

  private void checkState(boolean b, SourceLocation sourceLocation, String message, String... args) {
    if (!b) {
      throw new SqrlAstException(ErrorLabel.GENERIC,
          toParserPos(sourceLocation),
          message, args);
    }
  }

  private List<InferredField> walkChildren(ObjectTypeDefinition typeDef, SQRLTable table,
      List<InferredField> fields) {
    List<FieldDefinition> invalidFields = getInvalidFields(typeDef, table);
    //todo clean up, add lazy evaluation
    checkState(structurallyEqual(typeDef, table),
        invalidFields.isEmpty() ? typeDef.getSourceLocation() : invalidFields.get(invalidFields.size()-1).getSourceLocation(), "Field(s) not allowed [%s]",
        String.join(",", invalidFields.stream()
            .map(e->e.getName())
            .collect(Collectors.toList())), typeDef.getName());

    return typeDef.getFieldDefinitions().stream()
        .filter(f -> !visited.contains(f))
        .map(f -> walk(f, table.getField(Name.system(f.getName())).get(), fields, typeDef))
        .collect(Collectors.toList());
  }

  private InferredField walk(FieldDefinition fieldDefinition, Field field,
      List<InferredField> fields, ObjectTypeDefinition parent) {
    visited.add(fieldDefinition);
    if (field instanceof Relationship) {
      return walkRel(fieldDefinition, (Relationship) field, fields, parent);
    } else {
      return walkScalar(fieldDefinition, (Column) field, parent);
    }
  }

  private InferredField walkRel(FieldDefinition fieldDefinition, Relationship relationship,
      List<InferredField> fields, ObjectTypeDefinition parent) {
    return new NestedField(relationship,
        inferObjectField(fieldDefinition, relationship.getToTable(),
            fields, parent, relationship.getFromTable()));
  }

  private InferredField walkScalar(FieldDefinition fieldDefinition, Column column,
      ObjectTypeDefinition parent) {
    checkValidArrayNonNullType(fieldDefinition.getType());
    TypeDefinition type = unwrapObjectType(fieldDefinition.getType());
    //Todo: expand server to allow type coercion
    //Todo: enums
    Preconditions.checkState(type instanceof ScalarTypeDefinition,
        "Unknown type found");
    ScalarTypeDefinition scalarTypeDefinition = (ScalarTypeDefinition) type;
    SqlParserPos pos = toParserPos(fieldDefinition.getType().getSourceLocation());

    switch (scalarTypeDefinition.getName()) {
      case "Int":
        if (!SqlTypeName.INT_TYPES.contains(column.getType().getSqlTypeName())) {
          throw new SqrlAstException(ErrorLabel.GENERIC, pos, parent.getName() + ":"+fieldDefinition.getName()+"  expected SQRL Int type, found: %s",
              column.getType().getSqlTypeName().getName());
        }
        break;
      case "Float":
        if (!SqlTypeName.NUMERIC_TYPES.contains(column.getType().getSqlTypeName())) {
          throw new SqrlAstException(ErrorLabel.GENERIC, pos, "Expected SQRL Numeric type, found: %s",
              column.getType().getSqlTypeName().getName());
        }
        break;
      case "String":
        if (!SqlTypeName.STRING_TYPES.contains(column.getType().getSqlTypeName()) &&
            !SqlTypeName.DATETIME_TYPES.contains(column.getType().getSqlTypeName())) {
          throw new SqrlAstException(ErrorLabel.GENERIC, pos, "Expected SQRL String or Date type, found: %s",
              column.getType().getSqlTypeName().getName());
        }
        break;
      case "Boolean":
        if (!SqlTypeName.BOOLEAN_TYPES.contains(column.getType().getSqlTypeName())) {
          throw new SqrlAstException(ErrorLabel.GENERIC, pos, "Expected SQRL boolean type, found: %s",
              column.getType().getSqlTypeName().getName());
        }
        break;
      case "ID":
      default:
        throw new SqrlAstException(ErrorLabel.GENERIC,
            toParserPos(type.getSourceLocation()),
            "Unrecognized type: %s", column.getType().getSqlTypeName().getName());
    }

    return new InferredScalarField(fieldDefinition, column, parent);
  }

  private SqlParserPos toParserPos(SourceLocation sourceLocation) {
    return new SqlParserPos(sourceLocation.getLine(), sourceLocation.getColumn());
  }

  private void checkValidArrayNonNullType(Type type) {
    Type root = type;
    if (type instanceof NonNullType) {
      type = ((NonNullType) type).getType();
    }
    if (type instanceof ListType) {
      type = ((ListType) type).getType();
    }
    if (type instanceof NonNullType) {
      type = ((NonNullType) type).getType();
    }
    if (!(type instanceof TypeName)) {
      throw new SqrlAstException(ErrorLabel.GENERIC,
          toParserPos(root.getSourceLocation()),
          "Type must be a non-null array, array, or non-null");
    }
  }

  private boolean structurallyEqual(ImplementingTypeDefinition typeDef, SQRLTable table) {
    return typeDef.getFieldDefinitions().stream()
        .allMatch(f -> table.getField(Name.system(((NamedNode)f).getName())).isPresent());
  }

  private List<FieldDefinition> getInvalidFields(ObjectTypeDefinition typeDef, SQRLTable table) {
    return typeDef.getFieldDefinitions().stream()
        .filter(f -> table.getField(Name.system(f.getName())).isEmpty())
        .collect(Collectors.toList());
  }

  private TypeDefinition unwrapObjectType(Type type) {
    //type can be in a single array with any non-nulls, e.g. [customer!]!
    type = unboxNonNull(type);
    if (type instanceof ListType) {
      type = ((ListType) type).getType();
    }
    type = unboxNonNull(type);

    Optional<TypeDefinition> typeDef = this.registry.getType(type);

    checkState(typeDef.isPresent(), type.getSourceLocation(),
        "Could not find Object type [%s]", type instanceof TypeName ?
            ((TypeName) type).getName() : type.toString());

    return typeDef.get();
  }

  private boolean isListType(Type type) {
    return unboxNonNull(type) instanceof ListType;
  }

  private Type unboxNonNull(Type type) {
    if (type instanceof NonNullType) {
      return unboxNonNull(((NonNullType) type).getType());
    }
    return type;
  }

  private InferredMutations resolveMutations(ObjectTypeDefinition m) {
    List<InferredMutation> mutations = new ArrayList<>();
    for(FieldDefinition def : m.getFieldDefinitions()) {
      validateStructurallyEqualMutation(def, getValidMutationReturnType(def), getValidMutationInput(def),
          List.of(ReservedName.SOURCE_TIME.getCanonical()));
      TableSink tableSink = apiManager.getMutationSource(source, Name.system(def.getName()));
      if (tableSink==null) {
        throw new SqrlAstException(ErrorLabel.GENERIC, toParserPos(m.getSourceLocation()),
            "Could not find mutation source: %s.%s", def.getName());
      }
      //TODO: validate that tableSink schema matches Input type
      SerializedSqrlConfig config = tableSink.getConfiguration().getConfig().serialize();
      InferredMutation mutation = new InferredMutation(def.getName(), config);
      mutations.add(mutation);
    }

    return new InferredMutations(mutations);
  }

  private Object validateStructurallyEqualMutation(FieldDefinition def, ObjectTypeDefinition subType, InputObjectTypeDefinition type,
      List<String> allowedFieldNames) {


    //The return type can have _source_time
    for (FieldDefinition f : subType.getFieldDefinitions()) {
      if (allowedFieldNames.contains(f.getName())) {
        continue;
      }

      String name = f.getName();

      InputValueDefinition in = findExactlyOneInputValue(def, name, type.getInputValueDefinitions());

      //validate type structurally equal
      validateStructurallyEqualMutation(f, in);

    }

    return null;
  }

  private void validateStructurallyEqualMutation(FieldDefinition subType, InputValueDefinition type) {
    Preconditions.checkState(subType.getName().equals(type.getName()),
        "Name must be equal to the input name {} {}", subType.getName(), type.getName());
    Type subT = subType.getType();
    Type t = type.getType();

    validateStructurallyType(subType, subT, t);
  }

  private Object validateStructurallyType(FieldDefinition field, Type subT, Type t) {
    if (t instanceof NonNullType) {
      //subType may be nullable if type is non-null
      NonNullType nonNullType = (NonNullType) t;
      if (subT instanceof NonNullType) {
        NonNullType nonNullSubType = (NonNullType) subT;
        return validateStructurallyType(field, nonNullSubType.getType(), nonNullType.getType());
      } else {
        return validateStructurallyType(field, subT, nonNullType.getType());
      }
    } else if (t instanceof ListType) {
      //subType must be a list
      Preconditions.checkState(subT instanceof ListType, "List type mismatch for field. Must match the input type. " + field.getName());
      ListType listType = (ListType) t;
      ListType listSubType = (ListType) subT;
      return validateStructurallyType(field, listSubType, listType);
    } else if (t instanceof TypeName) {
      //If subtype nonnull then it could return errors
      Preconditions.checkState(!(subT instanceof NonNullType),
          "Non-null found on field %s, could result in errors if input type is null",
          field.getName());
      Preconditions.checkState(!(subT instanceof ListType),
          "List type found on field %s when the input is a scalar type",
          field.getName());

      //If typeName, resolve then
      TypeName typeName = (TypeName) t;
      TypeName subTypeName = (TypeName) subT;
      TypeDefinition typeDefinition = registry.getType(typeName)
          .orElseThrow(()->new RuntimeException("Could not find type: " + typeName.getName()));
      TypeDefinition subTypeDefinition = registry.getType(subTypeName)
          .orElseThrow(()->new RuntimeException("Could not find type: " + subTypeName.getName()));

      //If input or scalar
      if (typeDefinition instanceof ScalarTypeDefinition) {
        Preconditions.checkState(subTypeDefinition instanceof ScalarTypeDefinition &&
            typeDefinition.getName().equals(subTypeDefinition.getName()),
            "Scalar types not matching for field [%s]: %s %s", field.getName(),
            typeDefinition.getName(), subTypeDefinition.getName());
        return null;
      } else if (typeDefinition instanceof EnumTypeDefinition) {
        Preconditions.checkState(subTypeDefinition instanceof EnumTypeDefinition &&
                typeDefinition.getName().equals(subTypeDefinition.getName()),
            "Enum types not matching for field [%s]: %s %s", field.getName(),
            typeDefinition.getName(), subTypeDefinition.getName());
        return null;
      } else if (typeDefinition instanceof InputObjectTypeDefinition){
        Preconditions.checkState(subTypeDefinition instanceof ObjectTypeDefinition,
            "Return object type must match with an input object type not matching for field [%s]: %s %s", field.getName(),
            typeDefinition.getName(), subTypeDefinition.getName());
        ObjectTypeDefinition obj = (ObjectTypeDefinition) subTypeDefinition;
        InputObjectTypeDefinition in = (InputObjectTypeDefinition) typeDefinition;
        return validateStructurallyEqualMutation(field, obj, in, List.of());
      } else {
        throw new RuntimeException("Unknown type encountered: " + typeDefinition.getName());
      }
    }

    throw new RuntimeException("unexpected type");
  }

  private InputValueDefinition findExactlyOneInputValue(FieldDefinition def, String name,
      List<InputValueDefinition> inputValueDefinitions) {
    InputValueDefinition found = null;
    for (InputValueDefinition in : inputValueDefinitions) {
      if (in.getName().equals(name)) {
        if (found != null) {
          throw new RuntimeException("Too many fields found");
        }
        found = in;
      }
    }

    if (found == null) {
      throw new RuntimeException("Could not find field " + name + " in type " + def.getName());
    }

    return found;
  }

  private ObjectTypeDefinition getValidMutationReturnType(FieldDefinition def) {
    Type type = def.getType();
    if (type instanceof NonNullType) {
      type = ((NonNullType)type).getType();
    }

    Preconditions.checkState(type instanceof TypeName, def.getName() + " must be a singular return value");
    TypeName name = (TypeName) type;

    TypeDefinition typeDef = registry.getType(name)
        .orElseThrow(()->new RuntimeException("Could not find return type:" + name.getName()));
    Preconditions.checkState(typeDef instanceof ObjectTypeDefinition, "Return must be an object type:" + def.getName());

    return (ObjectTypeDefinition) typeDef;
  }

  private InputObjectTypeDefinition getValidMutationInput(FieldDefinition def) {
    checkState(!(def.getInputValueDefinitions().size() == 0), def.getSourceLocation(), def.getName() + " has too few arguments. Must have one non-null input type argument.");
    checkState(def.getInputValueDefinitions().size() == 1, def.getSourceLocation(), def.getName() + " has too many arguments. Must have one non-null input type argument.");
    checkState(def.getInputValueDefinitions().get(0).getType() instanceof NonNullType, def.getSourceLocation(),
        "[" + def.getName() + "] " + def.getInputValueDefinitions().get(0).getName()
              + "Must be non-null.");
    NonNullType nonNullType = (NonNullType)def.getInputValueDefinitions().get(0).getType();
    checkState(nonNullType.getType() instanceof TypeName, def.getSourceLocation(), "Must be a singular value");
    TypeName name = (TypeName) nonNullType.getType();

    Optional<TypeDefinition> typeDef = registry.getType(name);
    checkState(typeDef.isPresent(), def.getSourceLocation(), "Could not find input type:" + name.getName());
    checkState(typeDef.get() instanceof InputObjectTypeDefinition, def.getSourceLocation(), "Input must be an input object type:" + def.getName());

    return (InputObjectTypeDefinition) typeDef.get();
  }

  private InferredSubscriptions resolveSubscriptions(ObjectTypeDefinition s) {
    List<InferredSubscription> subscriptions = new ArrayList<>();
    //todo validation and nested queries
    List<InferredField> fields = new ArrayList<>();

    for(FieldDefinition def : s.getFieldDefinitions()) {
      SQRLTable table = resolveRootSQRLTable(def, def.getType(), def.getName(), "Subscription");
      APISubscription subscriptionDef = new APISubscription(Name.system(def.getName()),source);
      TableSource tableSource = apiManager.addSubscription(subscriptionDef, table);
      SerializedSqrlConfig kafkaSource = tableSource.getConfiguration().getConfig().serialize();
      //Resolve the queries for all nested entries, validate all fields are sqrl fields
      TypeDefinition returnType = registry.getType(def.getType())
          .orElseThrow(()->new RuntimeException("Could not find type"));
      //todo check before cast
      ObjectTypeDefinition objectTypeDefinition = (ObjectTypeDefinition) returnType;

      for (FieldDefinition definition : objectTypeDefinition.getFieldDefinitions()) {
        Field field = table.getField(Name.system(definition.getName()))
            .orElseThrow(() -> new RuntimeException("Unrecognized field " + definition.getName() +
                " in " + objectTypeDefinition.getName()));
        InferredField inferredField = walk(definition, field, fields, objectTypeDefinition);
        fields.add(inferredField);
      }

      InferredSubscription subscription = new InferredSubscription(def.getName(), kafkaSource);
      subscriptions.add(subscription);
    }
    return new InferredSubscriptions(subscriptions, fields);
  }


}
