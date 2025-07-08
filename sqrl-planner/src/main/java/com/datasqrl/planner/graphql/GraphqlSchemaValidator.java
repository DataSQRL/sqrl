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
package com.datasqrl.planner.graphql;

import static com.datasqrl.graphql.jdbc.SchemaConstants.LIMIT;
import static com.datasqrl.graphql.jdbc.SchemaConstants.OFFSET;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getQueryTypeName;
import static com.datasqrl.graphql.server.TypeDefinitionRegistryUtil.getType;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.checkState;
import static com.datasqrl.graphql.util.GraphqlCheckUtil.createThrowable;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.canonicalizer.ReservedName;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.graphql.APISource;
import com.datasqrl.planner.dag.plan.MutationQuery;
import com.datasqrl.planner.tables.SqrlFunctionParameter;
import com.datasqrl.planner.tables.SqrlTableFunction;
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
import graphql.language.TypeName;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLNamedType;
import graphql.schema.GraphQLNonNull;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.FunctionParameter;

/**
 * Validate that a graphQL schema is valid (used only to validate the user provided a graphQl
 * schema)
 */
public class GraphqlSchemaValidator extends GraphqlSchemaWalker {

  private final ErrorCollector errorCollector;

  @Inject
  public GraphqlSchemaValidator(
      List<SqrlTableFunction> tableFunctions,
      List<MutationQuery> mutations,
      ErrorCollector errorCollector) {
    super(tableFunctions, mutations);
    this.errorCollector = errorCollector;
  }

  @Override
  protected void visitSubscription(
      FieldDefinition atField, SqrlTableFunction tableFunction, TypeDefinitionRegistry registry) {
    checkArgumentsMatchParameters(atField, tableFunction, registry);
  }

  @Override
  protected void visitMutation(
      FieldDefinition atField, TypeDefinitionRegistry registry, MutationQuery mutation) {
    validateStructurallyEqualMutation(
        atField,
        getValidMutationOutputType(atField, registry),
        getValidMutationInputType(atField, registry),
        List.of(
            ReservedName.MUTATION_TIME.getCanonical(),
            ReservedName.MUTATION_PRIMARY_KEY.getDisplay()),
        registry);
  }

  private Object validateStructurallyEqualMutation(
      FieldDefinition field,
      ObjectTypeDefinition outputType,
      InputObjectTypeDefinition inputType,
      List<String> allowedFieldNames,
      TypeDefinitionRegistry registry) {

    for (FieldDefinition outputField : outputType.getFieldDefinitions()) {
      if (allowedFieldNames.contains(outputField.getName())) {
        continue;
      }

      var outputFieldName = outputField.getName();
      var inputField =
          findExactlyOneInputValue(field, outputFieldName, inputType.getInputValueDefinitions());

      validateStructurallyEqualFields(outputField, inputField, registry);
    }

    return null;
  }

  private void validateStructurallyEqualFields(
      FieldDefinition outputField,
      InputValueDefinition inputField,
      TypeDefinitionRegistry registry) {
    checkState(
        outputField.getName().equals(inputField.getName()),
        outputField.getSourceLocation(),
        "Name must be equal to the input name {} {}",
        outputField.getName(),
        inputField.getName());
    var outputType = outputField.getType();
    var inputType = inputField.getType();

    validateStructurallyEqualTypes(outputField, Optional.empty(), outputType, inputType, registry);
  }

  private Object validateStructurallyEqualTypes(
      FieldDefinition outputField,
      Optional<InputValueDefinition> argument,
      Type outputType,
      Type inputType,
      TypeDefinitionRegistry registry) {
    var argumentAppendix =
        argument.map(InputValueDefinition::getName).map(name -> "at argument " + name).orElse("");
    if (inputType instanceof NonNullType nonNullInputType) {
      if (outputType instanceof NonNullType nonNullOutputType) {
        return validateStructurallyEqualTypes(
            outputField,
            argument,
            nonNullOutputType.getType(),
            nonNullInputType.getType(),
            registry);
      } else {
        return validateStructurallyEqualTypes(
            outputField, argument, outputType, nonNullInputType.getType(), registry);
      }
    } else if (inputType instanceof ListType inputListType) {
      // subType must be a list
      checkState(
          outputType instanceof ListType,
          inputType.getSourceLocation(),
          "List type mismatch for field %s %s. Must match the input type.",
          outputField.getName(),
          argumentAppendix);
      var outputListType = (ListType) outputType;
      return validateStructurallyEqualTypes(
          outputField, argument, outputListType.getType(), inputListType.getType(), registry);
    } else if (inputType instanceof TypeName inputTypeName) {
      // If subtype nonnull then it could return errors
      checkState(
          !(outputType instanceof NonNullType),
          inputType.getSourceLocation(),
          "Field %s %s requires non-null type",
          outputField.getName(),
          argumentAppendix);
      checkState(
          !(outputType instanceof ListType),
          inputType.getSourceLocation(),
          "List type found on field %s %s when the input is a scalar type",
          outputField.getName(),
          argumentAppendix);
      var outputTypeName = (TypeName) unboxNonNull(outputType);
      var inputTypeDef =
          registry
              .getType(inputTypeName)
              .orElseThrow(
                  () ->
                      createThrowable(
                          inputTypeName.getSourceLocation(),
                          "Could not find type: %s",
                          inputTypeName.getName()));
      var outputTypeDef =
          registry
              .getType(outputTypeName)
              .orElseThrow(
                  () ->
                      createThrowable(
                          outputTypeName.getSourceLocation(),
                          "Could not find type: %s",
                          outputTypeName.getName()));

      if (inputTypeDef instanceof ScalarTypeDefinition) {
        checkState(
            outputTypeDef instanceof ScalarTypeDefinition
                && inputTypeDef.getName().equals(outputTypeDef.getName()),
            inputType.getSourceLocation(),
            "Scalar types not matching for field %s %s: found %s but wanted %s",
            outputField.getName(),
            argumentAppendix,
            inputTypeDef.getName(),
            outputTypeDef.getName());
        return null;
      } else if (inputTypeDef instanceof EnumTypeDefinition) {
        checkState(
            outputTypeDef instanceof EnumTypeDefinition
                || outputTypeDef instanceof ScalarTypeDefinition
                    && inputTypeDef.getName().equals(outputTypeDef.getName()),
            inputType.getSourceLocation(),
            "Enum types not matching for field %s %s: found %s but wanted %s",
            outputField.getName(),
            argumentAppendix,
            inputTypeDef.getName(),
            outputTypeDef.getName());
        return null;
      } else if (inputTypeDef instanceof InputObjectTypeDefinition inputObjectTypeDef) {
        checkState(
            outputTypeDef instanceof ObjectTypeDefinition,
            inputType.getSourceLocation(),
            "Object types not matching for field %s %s: found %s but wanted %s",
            outputField.getName(),
            argumentAppendix,
            inputTypeDef.getName(),
            outputTypeDef.getName());
        var outputObjectTypeDef = (ObjectTypeDefinition) outputTypeDef;
        // walk object types
        return validateStructurallyEqualMutation(
            outputField, outputObjectTypeDef, inputObjectTypeDef, List.of(), registry);
      } else {
        throw createThrowable(
            inputType.getSourceLocation(),
            "Unknown type encountered for field %s %s: %s",
            outputField.getName(),
            argumentAppendix,
            inputTypeDef.getName());
      }
    }
    throw createThrowable(
        inputType.getSourceLocation(),
        "Unknown type encountered for field %s %s",
        outputField.getName(),
        argumentAppendix);
  }

  private InputValueDefinition findExactlyOneInputValue(
      FieldDefinition field, String forName, List<InputValueDefinition> inputValueDefinitions) {
    InputValueDefinition found = null;
    for (InputValueDefinition inputDefinition : inputValueDefinitions) {
      if (inputDefinition.getName().equals(forName)) {
        checkState(found == null, inputDefinition.getSourceLocation(), "Duplicate fields found");
        found = inputDefinition;
      }
    }

    checkState(
        found != null,
        field.getSourceLocation(),
        "Could not find field %s in type %s",
        forName,
        field.getName());

    return found;
  }

  private InputObjectTypeDefinition getValidMutationInputType(
      FieldDefinition fieldDefinition, TypeDefinitionRegistry registry) {
    checkState(
        !(fieldDefinition.getInputValueDefinitions().isEmpty()),
        fieldDefinition.getSourceLocation(),
        fieldDefinition.getName()
            + " has too few arguments. Must have one non-null input type argument.");
    checkState(
        fieldDefinition.getInputValueDefinitions().size() == 1,
        fieldDefinition.getSourceLocation(),
        fieldDefinition.getName()
            + " has too many arguments. Must have one non-null input type argument.");
    final var inputValueDefinition = fieldDefinition.getInputValueDefinitions().get(0);
    checkState(
        inputValueDefinition.getType() instanceof NonNullType,
        fieldDefinition.getSourceLocation(),
        "["
            + fieldDefinition.getName()
            + "] "
            + inputValueDefinition.getName()
            + "Must be non-null.");
    var innerType = unwrapNullAndList(inputValueDefinition.getType());
    checkState(
        innerType instanceof TypeName,
        fieldDefinition.getSourceLocation(),
        "Must be a singular value");
    var typeName = (TypeName) innerType;

    var typeDef = registry.getType(typeName);
    checkState(
        typeDef.isPresent(),
        fieldDefinition.getSourceLocation(),
        "Could not find input type:" + typeName.getName());
    checkState(
        typeDef.get() instanceof InputObjectTypeDefinition,
        fieldDefinition.getSourceLocation(),
        "Input must be an input object type:" + fieldDefinition.getName());

    return (InputObjectTypeDefinition) typeDef.get();
  }

  private static Type unwrapNullAndList(Type type) {
    if (type instanceof NonNullType nullType) {
      return unwrapNullAndList(nullType.getType());
    } else if (type instanceof ListType listType) {
      return unwrapNullAndList(listType.getType());
    } else {
      return type;
    }
  }

  private ObjectTypeDefinition getValidMutationOutputType(
      FieldDefinition fieldDefinition, TypeDefinitionRegistry registry) {
    var type = fieldDefinition.getType();
    type = unwrapNullAndList(type);
    checkState(
        type instanceof TypeName,
        type.getSourceLocation(),
        "[%s] must be a singular output value",
        fieldDefinition.getName());

    var typeName = (TypeName) type;
    var typeDef =
        registry
            .getType(typeName)
            .orElseThrow(
                () ->
                    createThrowable(
                        typeName.getSourceLocation(),
                        "Could not find mutation output type: %s",
                        typeName.getName()));
    checkState(
        typeDef instanceof ObjectTypeDefinition,
        typeDef.getSourceLocation(),
        "Mutation output must be an object type: %s",
        fieldDefinition.getName());

    return (ObjectTypeDefinition) typeDef;
  }

  @Override
  protected void visitUnknownObject(FieldDefinition atField, Optional<RelDataType> relDataType) {
    throw createThrowable(
        atField.getSourceLocation(),
        "Unknown field at location %s",
        relDataType
            .map(
                r ->
                    atField.getName()
                        + ". Possible scalars are ["
                        + r.getFieldNames().stream()
                            .filter(
                                com.datasqrl.graphql.generate.GraphqlSchemaUtil::isValidGraphQLName)
                            .collect(Collectors.joining(", "))
                        + "]")
            .orElse(atField.getName()));
  }

  @Override
  protected void visitScalar(
      ObjectTypeDefinition objectType,
      FieldDefinition atField,
      RelDataTypeField relDataTypeField) {}

  @Override
  protected void visitQuery(
      ObjectTypeDefinition parentType,
      FieldDefinition atField,
      SqrlTableFunction tableFunction,
      TypeDefinitionRegistry registry) {
    checkValidArrayNonNullType(atField.getType());
    checkArgumentsMatchParameters(atField, tableFunction, registry);
  }

  private void checkArgumentsMatchParameters(
      FieldDefinition atField, SqrlTableFunction tableFunction, TypeDefinitionRegistry registry) {
    final var arguments = atField.getInputValueDefinitions();
    // Check that arguments match the table function parameters in name and types
    final List<FunctionParameter> externalParameters =
        tableFunction.getParameters().stream()
            .filter(parameter -> ((SqrlFunctionParameter) parameter).isExternalArgument())
            .collect(Collectors.toList());
    arguments.stream()
        .filter(argument -> !argument.getName().equals(LIMIT) && !argument.getName().equals(OFFSET))
        .forEach(
            argument -> {
              final var foundParameter =
                  externalParameters.stream()
                      .filter(parameter -> parameter.getName().equals(argument.getName()))
                      .findFirst();
              checkState(
                  foundParameter.isPresent(),
                  atField.getSourceLocation(),
                  "GraphQl argument [%s] not found in the parameters of [%s]",
                  argument.getName(),
                  atField.getName());

              // Infer the table function parameter type the same way schema inference does (and
              // with non-null wrapper as well) because there is a big chance that the user schema
              // is based on inference
              // TODO inject extendedScalarTypes conf parameter instead of passing true because if a
              // user provides a schema with id : Float and he disables extendedScalarTypes,
              // argument will type will be float and parameter type will be bigint
              final var inferedParameterType =
                  GraphqlSchemaUtil.getGraphQLInputType(
                      foundParameter.get().getType(null),
                      NamePath.of(foundParameter.get().getName()),
                      true);
              checkState(
                  inferedParameterType.isPresent(),
                  atField.getSourceLocation(),
                  "Cannot infer the type of parameter [%s]",
                  foundParameter.get().getName());
              var parameterType =
                  convertGraphQLInputTypeToType(inferedParameterType.get(), atField);

              // compare the inferred graphql parameter type to the argument type
              validateStructurallyEqualTypes(
                  atField, Optional.of(argument), parameterType, argument.getType(), registry);
            });
  }

  private Type convertGraphQLInputTypeToType(GraphQLInputType inputType, FieldDefinition atField) {
    if (inputType instanceof GraphQLNonNull nonNull) {
      return new NonNullType(
          convertGraphQLInputTypeToType((GraphQLInputType) nonNull.getWrappedType(), atField));
    } else if (inputType instanceof graphql.schema.GraphQLList list) {
      return new ListType(
          convertGraphQLInputTypeToType((GraphQLInputType) list.getWrappedType(), atField));
    } else if (inputType instanceof GraphQLNamedType namedType) {
      return new TypeName(namedType.getName());
    }
    throw createThrowable(
        atField.getSourceLocation(),
        "Unsupported GraphQLInputType %s",
        inputType.getClass().getName());
  }

  private void checkValidArrayNonNullType(Type type) {
    var root = type;
    if (type instanceof NonNullType nullType) {
      type = nullType.getType();
    }
    if (type instanceof ListType listType) {
      type = listType.getType();
    }
    if (type instanceof NonNullType nullType) {
      type = nullType.getType();
    }
    checkState(
        type instanceof TypeName,
        root.getSourceLocation(),
        "Type must be a non-null array, array, or non-null");
  }

  private Type unboxNonNull(Type type) {
    if (type
        instanceof
        NonNullType nullType) { // TODO this should be always be false in the first call due to
      // checkState(!(outputType instanceof NonNullType), ...)
      return unboxNonNull(nullType.getType());
    }
    return type;
  }

  public void validate(APISource source) {
    try {
      var registry = (new SchemaParser()).parse(source.getDefinition());
      var queryType = getType(registry, () -> getQueryTypeName(registry));
      if (queryType.isEmpty()) {
        throw createThrowable(null, "Cannot find graphql root Query type");
      }
      walkAPISource(source);
    } catch (Exception e) {
      throw errorCollector.handle(e);
    }
  }
}
