/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;
import static com.datasqrl.canonicalizer.Name.isSystemHidden;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.graphql.type.SqrlVertxScalars;
import com.datasqrl.json.GraphqlGeneratorMapping;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.util.ServiceLoaderDiscovery;
import graphql.Scalars;
import graphql.language.FieldDefinition;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@Slf4j
public class GraphqlSchemaUtil {
  public static final Map<Class, GraphqlGeneratorMapping> addlScalars = ServiceLoaderDiscovery
      .getAll(GraphqlGeneratorMapping.class)
      .stream()
      .collect(Collectors.toMap(GraphqlGeneratorMapping::getConversionClass, t->t));
  public static final Map<String, GraphQLScalarType> allScalars = createScalarMap();

  private static Map<String, GraphQLScalarType> createScalarMap() {
    return Stream.of(
        Scalars.GraphQLBoolean,
        Scalars.GraphQLFloat,
        Scalars.GraphQLInt,
        Scalars.GraphQLString,
        Scalars.GraphQLID,
        SqrlVertxScalars.JSON
    ).collect(Collectors.toMap(GraphQLScalarType::getName, t->t));
  }

  public static GraphQLOutputType wrap(GraphQLOutputType gqlType, RelDataType type) {
    if (!type.isNullable()) {
      return GraphQLNonNull.nonNull(gqlType);
    }
    return gqlType;
  }

  public static GraphQLOutputType wrap(GraphQLOutputType type, Multiplicity multiplicity) {
    switch (multiplicity) {
      case ZERO_ONE:
        return type;
      case ONE:
        return GraphQLNonNull.nonNull(type);
      case MANY:
      default:
        return GraphQLList.list(GraphQLNonNull.nonNull(type));
    }
  }

  public static boolean isValidGraphQLName(String name) {
    return !isSystemHidden(name) && Pattern.matches("[_A-Za-z][_0-9A-Za-z]*", name);
  }

  public static Optional<GraphQLInputType> getInputType(RelDataType type, NamePath namePath, Set<String> seen) {
    return getInOutType(type, namePath, seen)
        .map(f->(GraphQLInputType)f);
  }

  public static Optional<GraphQLOutputType> getOutputType(RelDataType type, NamePath namePath, Set<String> seen) {
    return getInOutType(type, namePath, seen)
        .map(f->(GraphQLOutputType)f);
  }

  public static Optional<GraphQLType> getInOutType(RelDataType type, NamePath namePath,
      Set<String> seen) {
    return getInOutTypeHelper(type, namePath, seen);
  }

  // Todo move to dialect?
  public static Optional<GraphQLType> getInOutTypeHelper(RelDataType type, NamePath namePath, 
      Set<String> seen) {
    if (type.getSqlTypeName() == null) {
      return Optional.empty();
    }

    switch (type.getSqlTypeName()) {
      case OTHER:
        if (type instanceof RawRelDataType) {
          RawRelDataType rawRelDataType = (RawRelDataType) type;
          Class<?> originatingClass = rawRelDataType.getRawType().getOriginatingClass();
          if (addlScalars.containsKey(originatingClass)) {
            String scalarName = addlScalars.get(originatingClass).getScalarName();
            GraphQLScalarType graphQLScalarType = allScalars.get(scalarName);
            if (graphQLScalarType == null) {
              log.warn("Graphql type not supported: {}", scalarName);
              return Optional.empty();
            }
            return Optional.of(graphQLScalarType);
          }
        }

        return Optional.empty();
      case BOOLEAN:
        return Optional.of(Scalars.GraphQLBoolean);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return Optional.of(Scalars.GraphQLInt);
      case BIGINT: //treat bigint as float to prevent overflow
      case DECIMAL:
      case FLOAT:
      case REAL:
      case DOUBLE:
        return Optional.of(Scalars.GraphQLFloat);
      case DATE:
      case TIME:
      case TIME_WITH_LOCAL_TIME_ZONE:
      case TIMESTAMP:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return Optional.of(CustomScalars.DATETIME);
      case INTERVAL_YEAR:
      case INTERVAL_YEAR_MONTH:
      case INTERVAL_MONTH:
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
      case CHAR:
      case VARCHAR:
        return Optional.of(Scalars.GraphQLString);
      case ARRAY:
      case MULTISET:
        return getOutputType(type.getComponentType(), namePath, seen).map(GraphQLList::list);
      case STRUCTURED:
      case ROW:
        GraphQLObjectType.Builder builder = GraphQLObjectType.newObject();
        String typeName = generateUniqueNameForType(namePath, seen, "Result"); // Ensure unique names for each ROW type
        builder.name(typeName);
        for (RelDataTypeField field : type.getFieldList()) {
          if (field.getName().startsWith(HIDDEN_PREFIX)) continue;
          getOutputType(field.getType(), namePath.concat(Name.system(field.getName())), seen)
              .ifPresent(fieldType -> builder.field(GraphQLFieldDefinition.newFieldDefinition()
                  .name(field.getName())
                  .type(wrap(fieldType, field.getType()))
                  .build()));
        }
        return Optional.of(builder.build());

      case BINARY:
      case VARBINARY:
      case NULL:
      case ANY:
      case SYMBOL:
      case DISTINCT:
      case MAP:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
      case SARG:
      default:
        return Optional.empty();
    }
  }
  public static final AtomicInteger i = new AtomicInteger(0);

  private static String generateUniqueNameForType(NamePath namePath, Set<String> seen, String postfix) {
    String name = toName(namePath, postfix);
    String uniqueName = uniquify(name, seen);
    seen.add(uniqueName);

    return uniqueName;
  }

  private static String uniquify(String name, Set<String> seen) {
    int i = 0;
    while (seen.contains(name + (i == 0 ? "" : i))) i++;
    return name + (i == 0 ? "" : i);
  }

  private static String toName(NamePath namePath, String postfix) {
    return namePath.stream()
        .map(n -> n.getDisplay())
        .collect(Collectors.joining("_")) + postfix;
  }

  public static Optional<GraphQLOutputType> createOutputTypeForRelDataType(RelDataType type,
      NamePath namePath, Set<String> seen) {
    if (!type.isNullable()) {
      return getOutputType(type, namePath, seen).map(GraphQLNonNull::nonNull);
    }
    return getOutputType(type, namePath, seen);
  }

  public static Optional<GraphQLInputType> createInputTypeForRelDataType(RelDataType type, NamePath namePath, Set<String> seen) {
    if (namePath.getLast().isHidden()) return Optional.empty();
    if (!type.isNullable()) {
      return getGraphQLInputType(type, namePath, seen).map(GraphQLNonNull::nonNull);
    }
    return getGraphQLInputType(type, namePath, seen);
  }

  private static Optional<GraphQLInputType> getGraphQLInputType(RelDataType type, NamePath namePath, Set<String> seen) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return Optional.of(Scalars.GraphQLBoolean);
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        return Optional.of(Scalars.GraphQLInt);
      case BIGINT:
      case FLOAT:
      case REAL:
      case DOUBLE:
      case DECIMAL:
        return Optional.of(Scalars.GraphQLFloat);
      case CHAR:
      case VARCHAR:
        return Optional.of(Scalars.GraphQLString);
      case DATE:
      case TIME:
      case TIMESTAMP:
        return Optional.of(CustomScalars.DATETIME);
      case BINARY:
      case VARBINARY:
        return Optional.of(Scalars.GraphQLString); // Typically handled as Base64 encoded strings
      case ARRAY:
        return type.getComponentType() != null
            ? getGraphQLInputType(type.getComponentType(), namePath, seen).map(GraphQLList::list)
            : Optional.empty();
      case ROW:
        return createGraphQLInputObjectType(type, namePath, seen);
      case MAP:
        // Maps can be represented as a list of key-value pairs if GraphQL does not natively support maps
        RelDataType keyType = type.getKeyType();
        RelDataType valueType = type.getValueType();
        if (keyType != null && valueType != null) {
          return createGraphQLInputMapType(keyType, valueType, namePath, seen);
        }
        return Optional.empty();
      default:
        return Optional.empty(); // Unsupported types are omitted
    }
  }

  /**
   * Creates a GraphQL input object type for a ROW type.
   */
  private static Optional<GraphQLInputType> createGraphQLInputObjectType(RelDataType rowType, NamePath namePath, Set<String> seen) {
    GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject();
    String typeName = generateUniqueNameForType(namePath, seen, "Input");
    builder.name(typeName);

    for (RelDataTypeField field : rowType.getFieldList()) {
      createInputTypeForRelDataType(field.getType(), namePath.concat(Name.system(field.getName())), seen)
          .ifPresent(fieldType -> builder.field(GraphQLInputObjectField.newInputObjectField()
              .name(field.getName())
              .type(fieldType)
              .build()));
    }
    return Optional.of(builder.build());
  }

  /**
   * Helper to handle map types, transforming them into a list of key-value pairs.
   */
  private static Optional<GraphQLInputType> createGraphQLInputMapType(RelDataType keyType, RelDataType valueType, NamePath namePath, Set<String> seen) {
    GraphQLInputObjectType mapEntryType = GraphQLInputObjectType.newInputObject()
        .name("MapEntry")
        .field(GraphQLInputObjectField.newInputObjectField()
            .name("key")
            .type(getGraphQLInputType(keyType, namePath, seen).orElse(Scalars.GraphQLString))
            .build())
        .field(GraphQLInputObjectField.newInputObjectField()
            .name("value")
            .type(getGraphQLInputType(valueType, namePath, seen).orElse(Scalars.GraphQLString))
            .build())
        .build();
    return Optional.of(GraphQLList.list(mapEntryType));
  }

  /**
   * Checks if the graphql schema has a different case than the graphql schema
   */
  public static boolean hasVaryingCase(FieldDefinition field, RelDataTypeField relDataTypeField) {
    return !field.getName().equals(relDataTypeField.getName());
  }
}
