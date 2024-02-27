package com.datasqrl.graphql.inference;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;
import static com.datasqrl.canonicalizer.Name.SYSTEM_HIDDEN_PREFIX;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getQueryTypeName;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getType;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.isValidGraphQLName;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.createThrowable;

import com.datasqrl.calcite.function.SqrlTableMacro;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.engine.database.inmemory.InMemoryMetadataStore;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.server.Model.ArgumentLookupCoords;
import com.datasqrl.graphql.server.TypeDefinitionRegistryUtil;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.queries.APISource;
import com.google.common.collect.Iterables;
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
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.schema.GraphQLTypeUtil;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Function;
import org.apache.calcite.sql.validate.SqlNameMatcher;

public class GraphqlSchemaValidator extends SchemaWalker {

  private Map<ObjectTypeDefinition, SqrlTableMacro> visitedObj = new HashMap<>();

  public GraphqlSchemaValidator(SqlNameMatcher nameMatcher, SqrlSchema schema, APISource source,
      TypeDefinitionRegistry registry, APIConnectorManager apiManager) {
    super(nameMatcher, schema, source, registry, apiManager);
  }

  @Override
  protected void walkSubscription(ObjectTypeDefinition m, FieldDefinition fieldDefinition) {
    //Assure they are root tables
    Collection<Function> functions = schema.getFunctions(fieldDefinition.getName(), false);
    checkState(functions.size() == 1, fieldDefinition.getSourceLocation(),
        "Cannot overload subscription");
    Function function = Iterables.getOnlyElement(functions);
    checkState(function instanceof SqrlTableMacro, fieldDefinition.getSourceLocation(),
        "Subscription not a sqrl table");

    //todo: validate that it is a valid table

  }

  @Override
  protected void walkMutation(ObjectTypeDefinition m, FieldDefinition fieldDefinition,
      TableSink tableSink) {
    // Check we've found the mutation
    TableSink mutationSink = apiManager.getMutationSource(source,
        Name.system(fieldDefinition.getName()));
    if (mutationSink == null) {
      throw createThrowable(fieldDefinition.getSourceLocation(),
          "Could not find mutation source: %s.", fieldDefinition.getName());
    }

    validateStructurallyEqualMutation(fieldDefinition, getValidMutationReturnType(fieldDefinition),
        getValidMutationInput(fieldDefinition), List.of(ReservedName.SOURCE_TIME.getCanonical()));

  }

  private Object validateStructurallyEqualMutation(FieldDefinition fieldDefinition,
      ObjectTypeDefinition returnTypeDefinition, InputObjectTypeDefinition inputType,
      List<String> allowedFieldNames) {

    //The return type can have _source_time
    for (FieldDefinition returnTypeFieldDefinition : returnTypeDefinition.getFieldDefinitions()) {
      if (allowedFieldNames.contains(returnTypeFieldDefinition.getName())) {
        continue;
      }

      String name = returnTypeFieldDefinition.getName();
      InputValueDefinition inputDefinition = findExactlyOneInputValue(fieldDefinition, name,
          inputType.getInputValueDefinitions());

      //validate type structurally equal
      validateStructurallyEqualMutation(returnTypeFieldDefinition, inputDefinition);
    }

    return null;
  }

  private void validateStructurallyEqualMutation(FieldDefinition fieldDefinition,
      InputValueDefinition inputDefinition) {
    checkState(fieldDefinition.getName().equals(inputDefinition.getName()),
        fieldDefinition.getSourceLocation(), "Name must be equal to the input name {} {}",
        fieldDefinition.getName(), inputDefinition.getName());
    Type definitionType = fieldDefinition.getType();
    Type inputType = inputDefinition.getType();

    validateStructurallyType(fieldDefinition, definitionType, inputType);
  }

  private Object validateStructurallyType(FieldDefinition field, Type definitionType,
      Type inputType) {
    if (inputType instanceof NonNullType) {
      //subType may be nullable if type is non-null
      NonNullType nonNullType = (NonNullType) inputType;
      if (definitionType instanceof NonNullType) {
        NonNullType nonNullDefinitionType = (NonNullType) definitionType;
        return validateStructurallyType(field, nonNullDefinitionType.getType(),
            nonNullType.getType());
      } else {
        return validateStructurallyType(field, definitionType, nonNullType.getType());
      }
    } else if (inputType instanceof ListType) {
      //subType must be a list
      checkState(definitionType instanceof ListType, definitionType.getSourceLocation(),
          "List type mismatch for field. Must match the input type. " + field.getName());
      ListType inputListType = (ListType) inputType;
      ListType definitionListType = (ListType) definitionType;
      return validateStructurallyType(field, definitionListType.getType(), inputListType.getType());
    } else if (inputType instanceof TypeName) {
      //If subtype nonnull then it could return errors
      checkState(!(definitionType instanceof NonNullType), definitionType.getSourceLocation(),
          "Non-null found on field %s, could result in errors if input type is null",
          field.getName());
      checkState(!(definitionType instanceof ListType), definitionType.getSourceLocation(),
          "List type found on field %s when the input is a scalar type", field.getName());

      //If typeName, resolve then
      TypeName inputTypeName = (TypeName) inputType;
      TypeName defTypeName = (TypeName) definitionType;
      TypeDefinition inputTypeDef = registry.getType(inputTypeName).orElseThrow(
          () -> createThrowable(inputTypeName.getSourceLocation(), "Could not find type: %s",
              inputTypeName.getName()));
      TypeDefinition defTypeDef = registry.getType(defTypeName).orElseThrow(
          () -> createThrowable(defTypeName.getSourceLocation(), "Could not find type: %s",
              defTypeName.getName()));

      //If input or scalar
      if (inputTypeDef instanceof ScalarTypeDefinition) {
        checkState(defTypeDef instanceof ScalarTypeDefinition && inputTypeDef.getName()
                .equals(defTypeDef.getName()), field.getSourceLocation(),
            "Scalar types not matching for field [%s]: found %s but wanted %s", field.getName(),
            inputTypeDef.getName(), defTypeDef.getName());
        return null;
      } else if (inputTypeDef instanceof EnumTypeDefinition) {
        checkState(defTypeDef instanceof EnumTypeDefinition
                || defTypeDef instanceof ScalarTypeDefinition && inputTypeDef.getName()
                .equals(defTypeDef.getName()), field.getSourceLocation(),
            "Enum types not matching for field [%s]: found %s but wanted %s", field.getName(),
            inputTypeDef.getName(), defTypeDef.getName());
        return null;
      } else if (inputTypeDef instanceof InputObjectTypeDefinition) {
        checkState(defTypeDef instanceof ObjectTypeDefinition, field.getSourceLocation(),
            "Return object type must match with an input object type not matching for field [%s]: found %s but wanted %s",
            field.getName(), inputTypeDef.getName(), defTypeDef.getName());
        ObjectTypeDefinition objectDefinition = (ObjectTypeDefinition) defTypeDef;
        InputObjectTypeDefinition inputDefinition = (InputObjectTypeDefinition) inputTypeDef;
        return validateStructurallyEqualMutation(field, objectDefinition, inputDefinition,
            List.of());
      } else {
        throw createThrowable(inputTypeDef.getSourceLocation(), "Unknown type encountered: %s",
            inputTypeDef.getName());
      }
    }

    throw createThrowable(field.getSourceLocation(), "Unknown type encountered for field: %s",
        field.getName());
  }

  private InputValueDefinition findExactlyOneInputValue(FieldDefinition fieldDefinition,
      String name, List<InputValueDefinition> inputValueDefinitions) {
    InputValueDefinition found = null;
    for (InputValueDefinition inputDefinition : inputValueDefinitions) {
      if (inputDefinition.getName().equals(name)) {
        checkState(found == null, inputDefinition.getSourceLocation(), "Duplicate fields found");
        found = inputDefinition;
      }
    }

    checkState(found != null, fieldDefinition.getSourceLocation(),
        "Could not find field %s in type %s", name, fieldDefinition.getName());

    return found;
  }

  private InputObjectTypeDefinition getValidMutationInput(FieldDefinition fieldDefinition) {
    checkState(!(fieldDefinition.getInputValueDefinitions().isEmpty()),
        fieldDefinition.getSourceLocation(), fieldDefinition.getName()
            + " has too few arguments. Must have one non-null input type argument.");
    checkState(fieldDefinition.getInputValueDefinitions().size() == 1,
        fieldDefinition.getSourceLocation(), fieldDefinition.getName()
            + " has too many arguments. Must have one non-null input type argument.");
    checkState(fieldDefinition.getInputValueDefinitions().get(0).getType() instanceof NonNullType,
        fieldDefinition.getSourceLocation(),
        "[" + fieldDefinition.getName() + "] " + fieldDefinition.getInputValueDefinitions().get(0)
            .getName() + "Must be non-null.");
    NonNullType nonNullType = (NonNullType) fieldDefinition.getInputValueDefinitions().get(0)
        .getType();
    checkState(nonNullType.getType() instanceof TypeName, fieldDefinition.getSourceLocation(),
        "Must be a singular value");
    TypeName name = (TypeName) nonNullType.getType();

    Optional<TypeDefinition> typeDef = registry.getType(name);
    checkState(typeDef.isPresent(), fieldDefinition.getSourceLocation(),
        "Could not find input type:" + name.getName());
    checkState(typeDef.get() instanceof InputObjectTypeDefinition,
        fieldDefinition.getSourceLocation(),
        "Input must be an input object type:" + fieldDefinition.getName());

    return (InputObjectTypeDefinition) typeDef.get();
  }


  private ObjectTypeDefinition getValidMutationReturnType(FieldDefinition fieldDefinition) {
    Type type = fieldDefinition.getType();
    if (type instanceof NonNullType) {
      type = ((NonNullType) type).getType();
    }

    checkState(type instanceof TypeName, type.getSourceLocation(),
        "[%s] must be a singular return value", fieldDefinition.getName());
    TypeName name = (TypeName) type;

    TypeDefinition typeDef = registry.getType(name).orElseThrow(
        () -> createThrowable(name.getSourceLocation(), "Could not find return type: %s"));
    checkState(typeDef instanceof ObjectTypeDefinition, typeDef.getSourceLocation(),
        "Return must be an object type: %s", fieldDefinition.getName());

    return (ObjectTypeDefinition) typeDef;
  }

  @Override
  protected void visitUnknownObject(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      Optional<RelDataType> rel) {
    throw createThrowable(field.getSourceLocation(), "Unknown field at location %s",
        rel.map(r-> field.getName() + ". Possible scalars are [" +
                r.getFieldNames().stream()
                    .filter(TypeDefinitionRegistryUtil::isValidGraphQLName)
                    .filter(f->!f.startsWith(SYSTEM_HIDDEN_PREFIX))
                    .collect(Collectors.joining(", ")) + "]")
            .orElse(field.getName()));
  }

  @Override
  protected Object visitScalar(ObjectTypeDefinition type, FieldDefinition field, NamePath path,
      RelDataType relDataType, RelDataTypeField relDataTypeField) {
    return null;
  }

  @Override
  protected ArgumentLookupCoords visitQuery(ObjectTypeDefinition parentType,
      ObjectTypeDefinition type, FieldDefinition field, NamePath path, Optional<RelDataType> rel,
      List<SqrlTableMacro> functions) {
    checkState(!functions.isEmpty(), field.getSourceLocation(), "Could not find functions");
    checkValidArrayNonNullType(field.getType());

//    if (visitedObj.get(type) != null && !visitedObj.get(type).getIsTypeOf()
//        .isEmpty()) {
      //todo readd the check to see if we can share a type
//      if (!sqrlTable.getIsTypeOf()
//          .contains(visitedObj.get(objectDefinition).getIsTypeOf().get(0))) {
//    checkState(visitedObj.get(parentType) == null,
//              || visitedObj.get(objectDefinition) == sqrlTable,
//          field.getSourceLocation(),
//          "Cannot redefine a type to point to a different SQRL table. Use an interface instead.\n"
//              + "The graphql field [%s] points to Sqrl table [%s] but already had [%s].",
//          parentType.getName() + ":" + field.getName(),
//          functions.get(0).getFullPath().getDisplay(),
//        visitedObj.get(parentType) == null ? null : visitedObj.get(parentType).getFullPath().getDisplay());
//      }
//    }
    visitedObj.put(parentType, functions.get(0));

    //todo: better structural checking
//    walkChildren((ObjectTypeDefinition) type, functions.get(0), field);
//    List<FieldDefinition> invalidFields = getInvalidFields(typeDef, table);
//    boolean structurallyEqual = structurallyEqual(typeDef, table);
//    //todo clean up, add lazy evaluation
//    checkState(structurallyEqual, invalidFields.isEmpty() ? typeDef.getSourceLocation()
//            : invalidFields.get(invalidFields.size() - 1).getSourceLocation(),
//        "Field(s) [%s] could not be found on type [%s]. Possible fields are: [%s]", String.join(",",
//            invalidFields.stream().map(FieldDefinition::getName).collect(Collectors.toList())),
//        typeDef.getName(), String.join(", ", table.tableMacro.getRowType().getFieldNames()));

    return null;
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
    checkState(type instanceof TypeName, root.getSourceLocation(),
        "Type must be a non-null array, array, or non-null");
  }

//  private boolean structurallyEqual(ImplementingTypeDefinition typeDef, SQRLTable table) {
//    return typeDef.getFieldDefinitions().stream()
//        .allMatch(f -> table.getField(Name.system(((NamedNode) f).getName())).isPresent());
//  }
//
//  private List<FieldDefinition> getInvalidFields(ObjectTypeDefinition typeDef, SQRLTable table) {
//    return typeDef.getFieldDefinitions().stream()
//        .filter(f -> table.getField(Name.system(f.getName())).isEmpty())
//        .collect(Collectors.toList());
//  }

  private TypeDefinition unwrapObjectType(Type type) {
    //type can be in a single array with any non-nulls, e.g. [customer!]!
    type = unboxNonNull(type);
    if (type instanceof ListType) {
      type = ((ListType) type).getType();
    }
    type = unboxNonNull(type);

    Optional<TypeDefinition> typeDef = this.registry.getType(type);

    checkState(typeDef.isPresent(), type.getSourceLocation(), "Could not find Object type [%s]",
        type instanceof TypeName ? ((TypeName) type).getName() : type.toString());

    return typeDef.get();
  }

  private Type unboxNonNull(Type type) {
    if (type instanceof NonNullType) {
      return unboxNonNull(((NonNullType) type).getType());
    }
    return type;
  }

  public void validate(APISource apiSchema, ErrorCollector errors) {
    try {
      errors = errors.withSchema(apiSchema.getName().getDisplay(), apiSchema.getSchemaDefinition());
      Optional<ObjectTypeDefinition> queryType = getType(registry,
          () -> getQueryTypeName(registry));
      if (queryType.isEmpty()) {
        throw createThrowable(null, "Cannot find graphql Query type");
      }

      walk();
    } catch (Exception e) {
      throw errors.handle(e);
    }
  }
}
