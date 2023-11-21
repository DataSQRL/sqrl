/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.graphql.inference.SqrlSchemaForInference.SQRLTable;
import com.datasqrl.graphql.type.SqrlVertxScalars;
import com.datasqrl.json.GraphqlGeneratorMapping;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Preconditions;
import com.google.common.collect.BiMap;
import graphql.Scalars;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@Slf4j
public class SchemaGeneratorUtil {
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

  public static String conformName(String name) {
    return name.replaceAll(
        "[^_a-zA-Z0-9]", "");
  }

  public static String getTypeName(SQRLTable table, BiMap<String, SQRLTable> names) {
    String conformed = conformName(table.getName());

    if (names.inverse().get(table) != null) {
      return names.inverse().get(table);
    }

    while (names.get(conformed) != null) {
      conformed = conformed + "_"; //add suffix
    }
    names.put(conformed, table);

    return conformed;
  }

  public static GraphQLTypeReference getTypeReference(SQRLTable table,
      BiMap<String, SQRLTable> names) {
    return new GraphQLTypeReference(getTypeName(table, names));
  }

  public static Optional<GraphQLInputType> getInputType(RelDataType type) {
    return getInOutType(type)
        .map(f->(GraphQLInputType)f);
  }

  public static Optional<GraphQLOutputType> getOutputType(RelDataType type) {
    return getInOutType(type)
        .map(f->(GraphQLOutputType)f);
  }

  public static Optional<GraphQLType> getInOutType(RelDataType type) {
    return getInOutTypeHelper(type);
  }

  public static Optional<GraphQLType> getInOutTypeHelper(RelDataType type) {
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
        return getOutputType(type.getComponentType()).map(GraphQLList::list);
      case STRUCTURED:
      case ROW:
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

  public static boolean isAccessible(SQRLTable table) {
    return !Name.isHiddenString(table.getName());
  }
}
