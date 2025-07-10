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
package com.datasqrl.graphql.converter;

import static graphql.Scalars.GraphQLString;

import com.datasqrl.graphql.server.operation.ApiOperation;
import com.datasqrl.graphql.server.operation.FunctionDefinition;
import com.datasqrl.graphql.server.operation.FunctionDefinition.Argument;
import com.datasqrl.graphql.server.operation.FunctionDefinition.Parameters;
import com.datasqrl.graphql.server.operation.GraphQLQuery;
import com.datasqrl.graphql.server.operation.McpMethodType;
import com.datasqrl.graphql.server.operation.RestMethodType;
import com.datasqrl.graphql.server.operation.ResultFormat;
import com.google.common.base.Preconditions;
import graphql.language.BooleanValue;
import graphql.language.Comment;
import graphql.language.Definition;
import graphql.language.Directive;
import graphql.language.Document;
import graphql.language.EnumValue;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.OperationDefinition;
import graphql.language.OperationDefinition.Operation;
import graphql.language.SourceLocation;
import graphql.language.StringValue;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.language.Value;
import graphql.language.VariableDefinition;
import graphql.parser.Parser;
import graphql.scalars.ExtendedScalars;
import graphql.schema.GraphQLArgument;
import graphql.schema.GraphQLDirective;
import graphql.schema.GraphQLEnumType;
import graphql.schema.GraphQLEnumValueDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLType;
import graphql.schema.idl.RuntimeWiring;
import graphql.schema.idl.SchemaGenerator;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.SchemaPrinter;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

/**
 * Converts a given GraphQL Schema to a tools configuration for the function backend. It extracts
 * all queries and mutations and converts them into {@link ApiOperation}.
 */
@RequiredArgsConstructor
@Slf4j
public class GraphQLSchemaConverter {

  SchemaPrinter schemaPrinter =
      new SchemaPrinter(SchemaPrinter.Options.defaultOptions().descriptionsAsHashComments(true));

  /**
   * Converts all operations defined within a given GraphQL operation definition string to an
   * equivalent list of API Functions.
   *
   * @param operationDefinition a string defining GraphQL operations
   * @param schema
   * @return a list of API Functions equivalent to the provided GraphQL operations
   * @throws IllegalArgumentException if operation definition contains no definitions or if an
   *     unexpected definition type is provided
   */
  public List<ApiOperation> convertOperations(
      String operationDefinition, GraphQLSchemaConverterConfig config, GraphQLSchema schema) {
    Parser parser = new Parser();
    Document document = parser.parseDocument(operationDefinition);
    Preconditions.checkArgument(
        !document.getDefinitions().isEmpty(), "Operation definition contains no definitions");

    List<ApiOperation> functions = new ArrayList<>();
    Iterator<Definition> defIter = document.getDefinitions().iterator();
    Definition definition = defIter.next();
    do {
      Preconditions.checkArgument(
          definition instanceof OperationDefinition,
          "Expected definition to be an operation, but got: %s",
          operationDefinition);
      OperationDefinition opDef = (OperationDefinition) definition;
      FunctionDefinition fctDef = convertOperationDefinition(opDef, schema);

      SourceLocation startLocation = definition.getSourceLocation();
      SourceLocation endLocation = new SourceLocation(Integer.MAX_VALUE, Integer.MAX_VALUE);
      definition = null;
      if (defIter.hasNext()) {
        definition = defIter.next();
        endLocation = definition.getSourceLocation();
      }
      // Get string between source and end location
      String queryString =
          extractOperation(
              operationDefinition,
              startLocation.getLine(),
              startLocation.getColumn(),
              endLocation.getLine(),
              endLocation.getColumn());
      GraphQLQuery query = new GraphQLQuery(queryString, fctDef.getName(), opDef.getOperation());
      ApiOperation.ApiOperationBuilder builder = ApiOperation.getBuilder(fctDef, query);
      config.setProtocolSupport(builder);
      applyApiArgs(toArgMap(opDef.getDirectives()), builder);
      functions.add(builder.build());
    } while (definition != null);
    return functions;
  }

  private static String extractOperation(
      String text, int startLine, int startColumn, int endLine, int endColumn) {
    String[] lines = text.split("\n");
    StringBuilder result = new StringBuilder();
    endLine = Math.min(endLine, lines.length);
    for (int i = startLine - 1; i <= endLine - 1; i++) {
      String line = lines[i];
      String subLine;
      if (i == startLine - 1 && i == endLine - 1) {
        subLine = line.substring(startColumn - 1, Math.min(endColumn - 1, line.length()));
      } else if (i == startLine - 1) {
        subLine = line.substring(startColumn - 1);
      } else if (i == endLine - 1) {
        subLine = (line.substring(0, Math.min(endColumn - 1, line.length())));
      } else {
        subLine = line;
      }
      int index = subLine.indexOf('#');
      subLine = (index != -1) ? subLine.substring(0, index) : subLine;
      if (!subLine.isBlank()) {
        result.append(subLine);
        result.append("\n");
      }
    }

    return result.toString();
  }

  private static String comments2String(List<Comment> comments) {
    return comments.stream().map(Comment::getContent).collect(Collectors.joining(" "));
  }

  /**
   * Converts a given GraphQL operation definition into a FunctionDefinition.
   *
   * @param node the OperationDefinition to be converted
   * @param schema
   * @return a FunctionDefinition that corresponds to the provided OperationDefinition
   */
  public FunctionDefinition convertOperationDefinition(
      OperationDefinition node, GraphQLSchema schema) {
    Operation op = node.getOperation();
    Preconditions.checkArgument(
        op == Operation.QUERY || op == Operation.MUTATION,
        "Do not yet support subscriptions: %s",
        node.getName());
    String fctComment = comments2String(node.getComments());
    String fctName = node.getName();

    FunctionDefinition funcDef = initializeFunctionDefinition(fctName, fctComment);
    Parameters params = funcDef.getParameters();

    // Process variable definitions
    List<VariableDefinition> variableDefinitions = node.getVariableDefinitions();
    for (VariableDefinition varDef : variableDefinitions) {
      String description = comments2String(varDef.getComments());
      String argName = varDef.getName();
      Type type = varDef.getType();

      boolean required = false;
      if (type instanceof NonNullType nonNullType) {
        required = true;
        type = nonNullType.getType();
      }
      Argument argDef = convert(type, schema);
      argDef.setDescription(description);
      if (required) params.getRequired().add(argName);
      params.getProperties().put(argName, argDef);
    }
    return funcDef;
  }

  private record OperationField(Operation op, GraphQLFieldDefinition fieldDefinition) {}

  /**
   * Converts the whole GraphQL schema into a list of {@link ApiOperation} instances.
   *
   * <p>This method will take the schema associated with this converter instance and convert every
   * query and mutation in the schema into an equivalent {@link ApiOperation}. The {@link
   * ApiOperation} instances are the ones that can be used by other parts of the system, acting as
   * an equivalent representation of the original GraphQL operations.
   *
   * @return List of {@link ApiOperation} instances corresponding to all the queries and mutations
   *     in the GraphQL schema.
   */
  public List<ApiOperation> convertSchema(
      GraphQLSchemaConverterConfig config, GraphQLSchema schema) {
    List<ApiOperation> functions = new ArrayList<>();

    List<GraphQLFieldDefinition> queries =
        Optional.ofNullable(schema.getQueryType())
            .map(GraphQLObjectType::getFieldDefinitions)
            .orElse(List.of());
    List<GraphQLFieldDefinition> mutations =
        Optional.ofNullable(schema.getMutationType())
            .map(GraphQLObjectType::getFieldDefinitions)
            .orElse(List.of());
    Stream.concat(
            queries.stream().map(fieldDef -> new OperationField(Operation.QUERY, fieldDef)),
            mutations.stream().map(fieldDef -> new OperationField(Operation.MUTATION, fieldDef)))
        .flatMap(
            input -> {
              if (config.getOperationFilter().test(input.op(), input.fieldDefinition().getName())) {
                try {
                  ApiOperation op = convert(input.op(), input.fieldDefinition(), config);
                  return Stream.of(op);
                } catch (Exception e) {
                  log.info(
                      "Operation could not be converted and is removed: {}",
                      input.fieldDefinition.getName(),
                      e);
                }
              } else { // else filter out
                log.info(
                    "Operation matches filter and is removed: {}", input.fieldDefinition.getName());
              }
              return Stream.of();
            })
        .forEach(functions::add);
    return functions;
  }

  private Argument convert(Type type, GraphQLSchema schema) {
    if (type instanceof NonNullType) {
      return convert(((NonNullType) type).getType(), schema);
    }
    Argument argument = new Argument();
    if (type instanceof ListType) {
      argument.setType("array");
      argument.setItems(convert(((ListType) type).getType(), schema));
    } else if (type instanceof TypeName) {
      String typeName = ((TypeName) type).getName();
      GraphQLType graphQLType = schema.getType(typeName);
      if (graphQLType instanceof GraphQLInputType graphQLInputType) {
        return GraphQLSchemaConverter.this.convert(graphQLInputType);
      } else {
        throw new UnsupportedOperationException(
            "Unexpected type [" + typeName + "] with class: " + graphQLType);
      }
    } else throw new UnsupportedOperationException("Unexpected type:  " + type);
    return argument;
  }

  private record Context(String opName, String prefix, int numArgs, List<GraphQLObjectType> path) {

    public Context nested(String fieldName, GraphQLObjectType type, int additionalArgs) {
      List<GraphQLObjectType> nestedPath = new ArrayList<>(path);
      nestedPath.add(type);
      return new Context(
          opName + "." + fieldName,
          combineStrings(prefix, fieldName),
          numArgs + additionalArgs,
          nestedPath);
    }

    public boolean isTopLevel() {
      return path.isEmpty();
    }
  }

  private static FunctionDefinition initializeFunctionDefinition(String name, String description) {
    FunctionDefinition funcDef = new FunctionDefinition();
    Parameters params = new Parameters();
    params.setType("object");
    params.setProperties(new HashMap<>());
    params.setRequired(new ArrayList<>());
    funcDef.setName(name);
    funcDef.setDescription(description);
    funcDef.setParameters(params);
    return funcDef;
  }

  private ApiOperation convert(
      Operation operationType,
      GraphQLFieldDefinition fieldDef,
      GraphQLSchemaConverterConfig config) {
    FunctionDefinition funcDef =
        initializeFunctionDefinition(
            config.getFunctionName(fieldDef.getName(), operationType), fieldDef.getDescription());
    Parameters params = funcDef.getParameters();
    String opName = operationType.name().toLowerCase() + "." + fieldDef.getName();
    StringBuilder queryHeader =
        new StringBuilder(operationType.name().toLowerCase())
            .append(" ")
            .append(fieldDef.getName())
            .append("(");
    StringBuilder queryBody = new StringBuilder();

    visit(fieldDef, queryBody, queryHeader, params, new Context(opName, "", 0, List.of()), config);

    queryHeader.append(") {\n").append(queryBody).append("\n}");
    GraphQLQuery apiQuery =
        new GraphQLQuery(queryHeader.toString(), fieldDef.getName(), operationType);
    ApiOperation.ApiOperationBuilder builder = ApiOperation.getBuilder(funcDef, apiQuery);
    config.setProtocolSupport(builder);
    applyApiArgs(toArgMap(fieldDef.getDirective("api")), builder);
    return builder.build();
  }

  private static String combineStrings(String prefix, String suffix) {
    return prefix + (prefix.isBlank() ? "" : "_") + suffix;
  }

  private record UnwrappedType(GraphQLInputType type, boolean required) {}

  private UnwrappedType convertRequired(GraphQLInputType type) {
    boolean required = false;
    if (type instanceof GraphQLNonNull) {
      required = true;
      type = (GraphQLInputType) ((GraphQLNonNull) type).getWrappedType();
    }
    return new UnwrappedType(type, required);
  }

  private Argument convert(GraphQLInputType graphQLInputType) {
    Argument argument = new Argument();
    if (graphQLInputType instanceof GraphQLScalarType) {
      argument.setType(convertScalarTypeToJsonType((GraphQLScalarType) graphQLInputType));
    } else if (graphQLInputType instanceof GraphQLEnumType enumType) {
      argument.setType("string");
      argument.setEnumValues(
          enumType.getValues().stream()
              .map(GraphQLEnumValueDefinition::getName)
              .collect(Collectors.toSet()));
    } else if (graphQLInputType instanceof GraphQLList) {
      argument.setType("array");
      argument.setItems(
          convert(
              convertRequired((GraphQLInputType) ((GraphQLList) graphQLInputType).getWrappedType())
                  .type()));
    } else if (graphQLInputType instanceof GraphQLInputObjectType inputObjectType) {
      argument.setType("object");
      Map<String, Argument> properties = new HashMap<>();
      List<String> required = new ArrayList<>();
      for (GraphQLInputObjectField field : inputObjectType.getFieldDefinitions()) {
        UnwrappedType unwrapped = convertRequired(field.getType());
        Argument nestedArg = convert(unwrapped.type());
        nestedArg.setDescription(field.getDescription());
        properties.put(field.getName(), nestedArg);
        if (unwrapped.required()) {
          required.add(field.getName());
        }
      }
      argument.setRequired(required);
      argument.setProperties(properties);
    } else {
      throw new UnsupportedOperationException("Unsupported type: " + graphQLInputType);
    }
    return argument;
  }

  public String convertScalarTypeToJsonType(GraphQLScalarType scalarType) {
    return switch (scalarType.getName()) {
      case "Int" -> "integer";
      case "Float" -> "number";
      case "String" -> "string";
      case "Boolean" -> "boolean";
      case "ID" -> "string"; // Typically treated as a string in JSON Schema
      default -> "string"; // We assume that type can be cast from string.
    };
  }

  public boolean visit(
      GraphQLFieldDefinition fieldDef,
      StringBuilder queryBody,
      StringBuilder queryHeader,
      Parameters params,
      Context ctx,
      GraphQLSchemaConverterConfig config) {
    GraphQLOutputType type = unwrapType(fieldDef.getType());
    if (type instanceof GraphQLObjectType) {
      GraphQLObjectType objectType = (GraphQLObjectType) type;
      // Don't recurse in a cycle or if depth limit is exceeded
      if (ctx.path().contains(objectType)) {
        log.info("Detected cycle on operation `{}`. Aborting traversal.", ctx.opName());
        return false;
      } else if (ctx.path().size() + 1 > config.getMaxDepth()) {
        log.info("Aborting traversal because depth limit exceeded on operation `{}`", ctx.opName());
        return false;
      }
    }
    if (ctx.isTopLevel() && config.hasTopLevelFieldAlias()) {
      queryBody.append(config.getTopLevelAlias()).append(": ");
    }
    queryBody.append(fieldDef.getName());
    int numArgs = 0;
    if (!fieldDef.getArguments().isEmpty()) {
      queryBody.append("(");
      for (Iterator<GraphQLArgument> args = fieldDef.getArguments().iterator(); args.hasNext(); ) {
        GraphQLArgument arg = args.next();

        UnwrappedType unwrappedType = convertRequired(arg.getType());
        // We flatten out
        if (unwrappedType.type() instanceof GraphQLInputObjectType inputType) {
          queryBody.append(arg.getName()).append(": { ");
          for (Iterator<GraphQLInputObjectField> nestedFields =
                  inputType.getFieldDefinitions().iterator();
              nestedFields.hasNext(); ) {
            GraphQLInputObjectField nestedField = nestedFields.next();
            String argName = combineStrings(ctx.prefix(), nestedField.getName());
            unwrappedType = convertRequired(nestedField.getType());
            argName =
                processField(
                    queryBody,
                    queryHeader,
                    params,
                    ctx,
                    unwrappedType,
                    argName,
                    nestedField.getName(),
                    nestedField.getDescription());
            String typeString = printFieldType(nestedField);
            if (numArgs > 0) queryHeader.append(", ");
            queryHeader.append(argName).append(": ").append(typeString);
            numArgs++;
            if (nestedFields.hasNext()) {
              queryBody.append(", ");
            }
          }
          queryBody.append(" }");
        } else {
          String argName = combineStrings(ctx.prefix(), arg.getName());
          argName =
              processField(
                  queryBody,
                  queryHeader,
                  params,
                  ctx,
                  unwrappedType,
                  argName,
                  arg.getName(),
                  arg.getDescription());
          String typeString = printArgumentType(arg);
          if (numArgs > 0) queryHeader.append(", ");
          queryHeader.append(argName).append(": ").append(typeString);
          numArgs++;
        }

        if (args.hasNext()) {
          queryBody.append(", ");
        }
      }
      queryBody.append(")");
    }
    if (type instanceof GraphQLObjectType) {
      GraphQLObjectType objectType = (GraphQLObjectType) type;
      List<GraphQLObjectType> nestedPath = new ArrayList<>(ctx.path());
      nestedPath.add(objectType);
      queryBody.append(" {\n");
      boolean atLeastOneField = false;
      for (GraphQLFieldDefinition nestedField : objectType.getFieldDefinitions()) {
        boolean success =
            visit(
                nestedField,
                queryBody,
                queryHeader,
                params,
                ctx.nested(nestedField.getName(), objectType, numArgs),
                config);
        atLeastOneField |= success;
      }
      Preconditions.checkArgument(
          atLeastOneField, "Expected at least on field on path: {}", ctx.opName());
      queryBody.append("}");
    }
    queryBody.append("\n");
    return true;
  }

  private String processField(
      StringBuilder queryBody,
      StringBuilder queryHeader,
      Parameters params,
      Context ctx,
      UnwrappedType unwrappedType,
      String argName,
      String originalName,
      String description) {
    Argument argDef = convert(unwrappedType.type());
    argDef.setDescription(description);
    if (unwrappedType.required()) params.getRequired().add(argName);
    Preconditions.checkArgument(
        !params.getProperties().containsKey(argName), "Duplicate argument name [%s]", argName);
    params.getProperties().put(argName, argDef);
    argName = "$" + argName;
    queryBody.append(originalName).append(": ").append(argName);
    return argName;
  }

  private String printFieldType(GraphQLInputObjectField field) {
    GraphQLInputObjectType type =
        GraphQLInputObjectType.newInputObject().name("DummyType").field(field).build();
    // Print argument as part of a dummy field in a dummy schema
    String output = schemaPrinter.print(type);
    return extractTypeFromDummy(output, field.getName());
  }

  private String printArgumentType(GraphQLArgument argument) {
    GraphQLArgument argumentWithoutDescription =
        argument.transform(builder -> builder.description(null));
    GraphQLObjectType type =
        GraphQLObjectType.newObject()
            .name("DummyType")
            .field(
                field ->
                    field
                        .name("dummyField")
                        .type(GraphQLString)
                        .argument(argumentWithoutDescription))
            .build();
    // Print argument as part of a dummy field in a dummy schema
    String output = schemaPrinter.print(type);
    return extractTypeFromDummy(output, argument.getName());
  }

  private String extractTypeFromDummy(String output, String fieldName) {
    // Remove comments
    output =
        Arrays.stream(output.split("\n"))
            .filter(line -> !line.trim().startsWith("#"))
            .collect(Collectors.joining("\n"));
    Pattern pattern = Pattern.compile(fieldName + "\\s*:\\s*([^)}]+)");
    // Print argument as part of a dummy field in a dummy schema
    Matcher matcher = pattern.matcher(output);
    Preconditions.checkArgument(matcher.find(), "Could not find type in: %s", output);
    return matcher.group(1).trim();
  }

  private static GraphQLOutputType unwrapType(GraphQLOutputType type) {
    if (type instanceof GraphQLList) {
      return unwrapType((GraphQLOutputType) ((GraphQLList) type).getWrappedType());
    } else if (type instanceof GraphQLNonNull) {
      return unwrapType((GraphQLOutputType) ((GraphQLNonNull) type).getWrappedType());
    } else {
      return type;
    }
  }

  // === Consolidated @api directive handling ====================================================
  private void applyApiArgs(Map<String, Object> args, ApiOperation.ApiOperationBuilder builder) {
    if (args == null || args.isEmpty()) return;
    Object v;
    if ((v = args.get("mcp")) != null) {
      builder.mcpMethod(McpMethodType.valueOf(v.toString().toUpperCase()));
    }
    if ((v = args.get("rest")) != null) {
      builder.restMethod(RestMethodType.valueOf(v.toString().toUpperCase()));
    }
    if ((v = args.get("format")) != null) {
      builder.format(ResultFormat.valueOf(v.toString().toUpperCase()));
    }
    if ((v = args.get("uri")) != null) builder.uriTemplate(v.toString());
  }

  private Map<String, Object> toArgMap(List<Directive> directives) {
    Map<String, Object> map = new HashMap<>();
    if (directives == null) return map;
    for (Directive dir : directives) {
      if (!"api".equals(dir.getName())) continue;
      dir.getArguments()
          .forEach(arg -> map.put(arg.getName(), convertLanguageValue(arg.getValue())));
    }
    return map;
  }

  private Map<String, Object> toArgMap(GraphQLDirective directive) {
    Map<String, Object> map = new HashMap<>();
    if (directive == null) return map;
    directive
        .getArguments()
        .forEach(
            arg ->
                map.put(
                    arg.getName(),
                    convertLanguageValue((Value<?>) arg.getArgumentValue().getValue())));
    return map;
  }

  private Object convertLanguageValue(Value<?> value) {
    if (value == null) return null;
    if (value instanceof BooleanValue bv) return bv.isValue();
    if (value instanceof StringValue sv) return sv.getValue();
    if (value instanceof EnumValue ev) return ev.getName();
    // Fallback to string representation
    return value.toString();
  }

  public GraphQLSchema getSchema(String schemaString) {
    TypeDefinitionRegistry typeRegistry = new SchemaParser().parse(schemaString);
    RuntimeWiring.Builder runtimeWiringBuilder = RuntimeWiring.newRuntimeWiring();
    getExtendedScalars().forEach(runtimeWiringBuilder::scalar);

    return new SchemaGenerator().makeExecutableSchema(typeRegistry, runtimeWiringBuilder.build());
  }

  private List<GraphQLScalarType> getExtendedScalars() {
    List<GraphQLScalarType> scalars = new ArrayList<>();

    Field[] fields = ExtendedScalars.class.getFields();
    for (Field field : fields) {
      if (Modifier.isPublic(field.getModifiers())
          && Modifier.isStatic(field.getModifiers())
          && GraphQLScalarType.class.isAssignableFrom(field.getType())) {
        try {
          scalars.add((GraphQLScalarType) field.get(null));
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }

    return scalars;
  }
}
