package com.datasqrl.v2.graphql;

import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getQueryTypeName;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getType;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.createThrowable;

import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.generate.GraphqlSchemaUtil;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.v2.dag.plan.MutationQuery;
import com.datasqrl.v2.tables.SqrlTableFunction;
import com.google.inject.Inject;
import graphql.language.EnumTypeDefinition;
import graphql.language.FieldDefinition;
import graphql.language.InputObjectTypeDefinition;
import graphql.language.InputValueDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeDefinition;
import graphql.language.TypeName;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

/**
 * Validate that a graphQL schema is valid (used only to validate the user provided a graphQl schema)
 */
public class GraphqlSchemaValidator2 extends GraphqlSchemaWalker2 {

  private final ErrorCollector errorCollector;

  @Inject
  public GraphqlSchemaValidator2(List<SqrlTableFunction> tableFunctions, List<MutationQuery> mutations, ErrorCollector errorCollector) {
    super(tableFunctions, mutations);
    this.errorCollector = errorCollector;
  }

  @Override
  protected void visitSubscription(FieldDefinition atField, SqrlTableFunction tableFunction) {
  }

  @Override
  protected void visitMutation(FieldDefinition atField, TypeDefinitionRegistry registry, MutationQuery mutation) {
    validateStructurallyEqualMutation(
            atField,
        getValidMutationOutputType(atField, registry),
        getValidMutationInputType(atField, registry),
        List.of(ReservedName.MUTATION_TIME.getCanonical(), ReservedName.MUTATION_PRIMARY_KEY.getDisplay()),
        registry);
  }

  private Object validateStructurallyEqualMutation(FieldDefinition field,
      ObjectTypeDefinition outputType, InputObjectTypeDefinition inputType,
      List<String> allowedFieldNames, TypeDefinitionRegistry registry) {

    for (FieldDefinition outputField : outputType.getFieldDefinitions()) {
      if (allowedFieldNames.contains(outputField.getName())) {
        continue;
      }

      String outputFieldName = outputField.getName();
      InputValueDefinition inputField = findExactlyOneInputValue(field, outputFieldName, inputType.getInputValueDefinitions());

      validateStructurallyEqualFields(outputField, inputField, registry);
    }

    return null;
  }

  private void validateStructurallyEqualFields(FieldDefinition outputField, InputValueDefinition inputField, TypeDefinitionRegistry registry) {
    checkState(outputField.getName().equals(inputField.getName()),
        outputField.getSourceLocation(), "Name must be equal to the input name {} {}",
        outputField.getName(), inputField.getName());
    Type outputType = outputField.getType();
    Type inputType = inputField.getType();

    validateStructurallyEqualTypes(outputField, outputType, inputType, registry);
  }

  private Object validateStructurallyEqualTypes(FieldDefinition outputField, Type outputType, Type inputType, TypeDefinitionRegistry registry) {
    if (inputType instanceof NonNullType) {
      //subType may be nullable if type is non-null
      NonNullType nonNullInputType = (NonNullType) inputType;
      if (outputType instanceof NonNullType) {
        NonNullType nonNullOutputType = (NonNullType) outputType;
        return validateStructurallyEqualTypes(outputField, nonNullOutputType.getType(), nonNullInputType.getType(), registry);
      } else {
        return validateStructurallyEqualTypes(outputField, outputType, nonNullInputType.getType(), registry);
      }
    } else if (inputType instanceof ListType) {
      //subType must be a list
      checkState(outputType instanceof ListType, outputType.getSourceLocation(),
          "List type mismatch for field. Must match the input type. " + outputField.getName());
      ListType inputListType = (ListType) inputType;
      ListType outputListType = (ListType) outputType;
      return validateStructurallyEqualTypes(outputField, outputListType.getType(), inputListType.getType(), registry);
    } else if (inputType instanceof TypeName) {
      //If subtype nonnull then it could return errors
      checkState(!(outputType instanceof NonNullType), outputType.getSourceLocation(),
          "Non-null found on field %s, could result in errors if input type is null",
          outputField.getName());
      checkState(!(outputType instanceof ListType), outputType.getSourceLocation(),
          "List type found on field %s when the input is a scalar type", outputField.getName());

      //If typeName, resolve then
      TypeName inputTypeName = (TypeName) inputType;
      TypeName outputTypeName = (TypeName) unboxNonNull(outputType);
      TypeDefinition inputTypeDef = registry.getType(inputTypeName).orElseThrow(
          () -> createThrowable(inputTypeName.getSourceLocation(), "Could not find type: %s", inputTypeName.getName()));
      TypeDefinition outputTypeDef = registry.getType(outputTypeName).orElseThrow(
          () -> createThrowable(outputTypeName.getSourceLocation(), "Could not find type: %s", outputTypeName.getName()));

      if (inputTypeDef instanceof ScalarTypeDefinition) {
        checkState(outputTypeDef instanceof ScalarTypeDefinition && inputTypeDef.getName()
                .equals(outputTypeDef.getName()), outputField.getSourceLocation(),
            "Scalar types not matching for field [%s]: found %s but wanted %s", outputField.getName(),
            inputTypeDef.getName(), outputTypeDef.getName());
        return null;
      } else if (inputTypeDef instanceof EnumTypeDefinition) {
        checkState(outputTypeDef instanceof EnumTypeDefinition
                || outputTypeDef instanceof ScalarTypeDefinition && inputTypeDef.getName()
                .equals(outputTypeDef.getName()), outputField.getSourceLocation(),
            "Enum types not matching for field [%s]: found %s but wanted %s", outputField.getName(),
            inputTypeDef.getName(), outputTypeDef.getName());
        return null;
      } else if (inputTypeDef instanceof InputObjectTypeDefinition) {
        checkState(outputTypeDef instanceof ObjectTypeDefinition, outputField.getSourceLocation(),
            "Return object type must match with an input object type not matching for field [%s]: found %s but wanted %s",
            outputField.getName(), inputTypeDef.getName(), outputTypeDef.getName());
        ObjectTypeDefinition outputObjectTypeDef = (ObjectTypeDefinition) outputTypeDef;
        InputObjectTypeDefinition inputObjectTypeDef = (InputObjectTypeDefinition) inputTypeDef;
        // walk object types
        return validateStructurallyEqualMutation(outputField, outputObjectTypeDef, inputObjectTypeDef, List.of(), registry);
      } else {
        throw createThrowable(inputTypeDef.getSourceLocation(), "Unknown type encountered: %s", inputTypeDef.getName());
      }
    }
    throw createThrowable(outputField.getSourceLocation(), "Unknown type encountered for field: %s", outputField.getName());
  }

  private InputValueDefinition findExactlyOneInputValue(FieldDefinition field, String forName, List<InputValueDefinition> inputValueDefinitions) {
    InputValueDefinition found = null;
    for (InputValueDefinition inputDefinition : inputValueDefinitions) {
      if (inputDefinition.getName().equals(forName)) {
        checkState(found == null, inputDefinition.getSourceLocation(), "Duplicate fields found");
        found = inputDefinition;
      }
    }

    checkState(found != null, field.getSourceLocation(),
        "Could not find field %s in type %s", forName, field.getName());

    return found;
  }

  private InputObjectTypeDefinition getValidMutationInputType(FieldDefinition fieldDefinition, TypeDefinitionRegistry registry) {
    checkState(!(fieldDefinition.getInputValueDefinitions().isEmpty()),
        fieldDefinition.getSourceLocation(), fieldDefinition.getName()
            + " has too few arguments. Must have one non-null input type argument.");
    checkState(fieldDefinition.getInputValueDefinitions().size() == 1,
        fieldDefinition.getSourceLocation(), fieldDefinition.getName()
            + " has too many arguments. Must have one non-null input type argument.");
    final InputValueDefinition inputValueDefinition = fieldDefinition.getInputValueDefinitions().get(0);
    checkState(inputValueDefinition.getType() instanceof NonNullType,
        fieldDefinition.getSourceLocation(),
        "[" + fieldDefinition.getName() + "] " + inputValueDefinition.getName() + "Must be non-null.");
    NonNullType nonNullType = (NonNullType) inputValueDefinition.getType();
    checkState(nonNullType.getType() instanceof TypeName, fieldDefinition.getSourceLocation(),
        "Must be a singular value");
    TypeName typeName = (TypeName) nonNullType.getType();

    Optional<TypeDefinition> typeDef = registry.getType(typeName);
    checkState(typeDef.isPresent(), fieldDefinition.getSourceLocation(),
        "Could not find input type:" + typeName.getName());
    checkState(typeDef.get() instanceof InputObjectTypeDefinition,
        fieldDefinition.getSourceLocation(),
        "Input must be an input object type:" + fieldDefinition.getName());

    return (InputObjectTypeDefinition) typeDef.get();
  }


  private ObjectTypeDefinition getValidMutationOutputType(FieldDefinition fieldDefinition, TypeDefinitionRegistry registry) {
    Type type = fieldDefinition.getType();

    if (type instanceof NonNullType) {
      type = ((NonNullType) type).getType();
    }
    checkState(type instanceof TypeName, type.getSourceLocation(),
        "[%s] must be a singular output value", fieldDefinition.getName());

    TypeName typeName = (TypeName) type;
    TypeDefinition typeDef =
        registry.getType(typeName)
            .orElseThrow(
                () -> createThrowable(
                        typeName.getSourceLocation(),
                        "Could not find mutation output type: %s",
                        typeName.getName())
            );
    checkState(typeDef instanceof ObjectTypeDefinition, typeDef.getSourceLocation(),
        "Mutation output must be an object type: %s", fieldDefinition.getName());

    return (ObjectTypeDefinition) typeDef;
  }

  @Override
  protected void visitUnknownObject(FieldDefinition atField, Optional<RelDataType> relDataType) {
    throw createThrowable(
        atField.getSourceLocation(), "Unknown field at location %s",
        relDataType.map(r ->
                    atField.getName() + ". Possible scalars are [" + r.getFieldNames().stream()
                                                                  .filter(GraphqlSchemaUtil::isValidGraphQLName)
                                                                  .collect(Collectors.joining(", "))
                                                            + "]")
            .orElse(atField.getName()));
  }

  @Override
  protected void visitScalar(ObjectTypeDefinition objectType, FieldDefinition atField, RelDataTypeField relDataTypeField) {
  }

  @Override
  protected void visitQuery(ObjectTypeDefinition parentType, FieldDefinition atField, SqrlTableFunction tableFunction) {
    checkValidArrayNonNullType(atField.getType());
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

  private Type unboxNonNull(Type type) {
    if (type instanceof NonNullType) { //TODO this should be always be false in the first call due to checkState(!(outputType instanceof NonNullType), ...)
      return unboxNonNull(((NonNullType) type).getType());
    }
    return type;
  }

  public void validate(APISource source) {
    try {
      TypeDefinitionRegistry registry = (new SchemaParser()).parse(source.getSchemaDefinition());
      Optional<ObjectTypeDefinition> queryType = getType(registry, () -> getQueryTypeName(registry));
      if (queryType.isEmpty()) {
        throw createThrowable(null, "Cannot find graphql root Query type");
      }
      walkAPISource(source);
    } catch (Exception e) {
      throw errorCollector.handle(e);
    }
  }
}
