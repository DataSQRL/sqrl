/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.v2.graphql;

import static com.datasqrl.canonicalizer.Name.HIDDEN_PREFIX;
import static com.datasqrl.canonicalizer.Name.isSystemHidden;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.graphql.server.CustomScalars;
import com.datasqrl.json.FlinkJsonType;
import com.datasqrl.schema.Multiplicity;
import graphql.Scalars;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLInputObjectField;
import graphql.schema.GraphQLInputObjectType;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import java.util.Optional;
import java.util.regex.Pattern;

import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.flink.table.planner.plan.schema.RawRelDataType;

@Slf4j
public class GraphqlSchemaUtil2 {

  public static GraphQLType wrapNullable(GraphQLType gqlType, RelDataType type) {
    if (!type.isNullable()) {
      return GraphQLNonNull.nonNull(gqlType);
    }
    return gqlType;
  }

  public static GraphQLType wrapMultiplicity(GraphQLType type, Multiplicity multiplicity) {
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

  public static Optional<GraphQLInputType> getInputType(RelDataType type, NamePath namePath, boolean extendedScalarTypes) {
    return getInOutType(type, namePath, extendedScalarTypes)
        .map(f->(GraphQLInputType)f);
  }

  public static Optional<GraphQLOutputType> getOutputType(RelDataType type, NamePath namePath, boolean extendedScalarTypes) {
    return getInOutType(type, namePath, extendedScalarTypes)
        .map(f->(GraphQLOutputType)f);
  }

  public static Optional<GraphQLType> getInOutType(RelDataType type, NamePath namePath, boolean extendedScalarTypes) {
    if (type.getSqlTypeName() == null) {
      return Optional.empty();
    }

    switch (type.getSqlTypeName()) {
      case OTHER:
        if (type instanceof RawRelDataType) {
          RawRelDataType rawRelDataType = (RawRelDataType) type;
          Class<?> originatingClass = rawRelDataType.getRawType().getOriginatingClass();
          if (originatingClass.isAssignableFrom(FlinkJsonType.class)) {
            return Optional.of(CustomScalars.JSON);
          }
        }

        return Optional.empty();
      case BOOLEAN:
        return Optional.of(Scalars.GraphQLBoolean);
      case TINYINT:
      case SMALLINT:
      case INTEGER:
        return Optional.of(Scalars.GraphQLInt);
      case BIGINT:
        if (extendedScalarTypes) {
          return Optional.of(CustomScalars.GRAPHQL_BIGINTEGER);
        }
      case DECIMAL:
      case FLOAT:
      case REAL:
      case DOUBLE:
        return Optional.of(Scalars.GraphQLFloat);
      case DATE:
        return Optional.of(CustomScalars.DATE);
      case TIME:
        return Optional.of(CustomScalars.TIME);
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
      // arity many, create a GraphQLList of the component type
      case ARRAY:
      case MULTISET:
        return getOutputType(type.getComponentType(), namePath, extendedScalarTypes).map(GraphQLList::list);
     // nested type, arity 1
      case STRUCTURED:
      case ROW:
        return createGraphQLOutputStructuredType(type, namePath, extendedScalarTypes);
      case MAP:
        return Optional.of(CustomScalars.JSON);
      case BINARY:
      case VARBINARY:
      case NULL:
      case ANY:
      case SYMBOL:
      case DISTINCT:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
      case SARG:
      default:
        return Optional.empty();
    }
  }


  //TODO will be used for mutations. Do not remove, inline when coding mutations
  public static Optional<GraphQLOutputType> createOutputTypeForRelDataType(RelDataType type, NamePath namePath, boolean extendedScalarTypes) {
    return getOutputType(type, namePath, extendedScalarTypes).map(t -> (GraphQLOutputType) wrapNullable(t, type));
  }

  private static Optional<GraphQLInputType> getGraphQLInputType(RelDataType type, NamePath namePath, boolean extendedScalarTypes) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return Optional.of(Scalars.GraphQLBoolean);
      case INTEGER:
      case SMALLINT:
      case TINYINT:
        return Optional.of(Scalars.GraphQLInt);
      case BIGINT:
        if (extendedScalarTypes) {
          return Optional.of(CustomScalars.GRAPHQL_BIGINTEGER);
        }
      case FLOAT:
      case REAL:
      case DOUBLE:
      case DECIMAL:
        return Optional.of(Scalars.GraphQLFloat);
      case CHAR:
      case VARCHAR:
        return Optional.of(Scalars.GraphQLString);
      case DATE:
        return Optional.of(CustomScalars.DATE);
      case TIME:
        return Optional.of(CustomScalars.TIME);
      case TIMESTAMP:
        return Optional.of(CustomScalars.DATETIME);
      case BINARY:
      case VARBINARY:
        return Optional.of(Scalars.GraphQLString); // Typically handled as Base64 encoded strings
      case ARRAY:
        return type.getComponentType() != null // traverse inner type
            ? getGraphQLInputType(type.getComponentType(), namePath, extendedScalarTypes).map(GraphQLList::list)
            : Optional.empty();
      case ROW:
        return createGraphQLInputStructuredType(type, namePath, extendedScalarTypes);
      case MAP:
        return Optional.of(CustomScalars.JSON);
      default:
        return Optional.empty(); // Unsupported types are omitted
    }
  }

  /**
   * Creates a GraphQL input object type for a ROW type.
   */
  private static Optional<GraphQLInputType> createGraphQLInputStructuredType(RelDataType rowType, NamePath namePath, boolean extendedScalarTypes) {
    GraphQLInputObjectType.Builder builder = GraphQLInputObjectType.newInputObject();
    String typeName = uniquifyNameForPath(namePath, "Input");
    builder.name(typeName);

    for (RelDataTypeField field : rowType.getFieldList()) {
      final NamePath fieldPath = namePath.concat(Name.system(field.getName()));
      if (namePath.getLast().isHidden()) continue;
      RelDataType type = field.getType();
      getGraphQLInputType(type, fieldPath, extendedScalarTypes).map(t -> (GraphQLInputType) wrapNullable(t, type))
          .ifPresent(fieldType -> builder.field(GraphQLInputObjectField.newInputObjectField()
              .name(field.getName())
              .type(fieldType)
              .build()));
    }
    return Optional.of(builder.build());
  }

  /**
   * Creates a GraphQL output object type for a ROW type.
   */
  private static Optional<GraphQLType> createGraphQLOutputStructuredType(RelDataType type, NamePath namePath, boolean extendedScalarTypes) {
    GraphQLObjectType.Builder builder = GraphQLObjectType.newObject();
    String typeName = uniquifyNameForPath(namePath, "Output");
    builder.name(typeName);
    for (RelDataTypeField field : type.getFieldList()) {
      if (field.getName().startsWith(HIDDEN_PREFIX)) continue;
      getOutputType(field.getType(), namePath.concat(Name.system(field.getName())), extendedScalarTypes) // recursively traverse
              .ifPresent(fieldType -> builder.field(GraphQLFieldDefinition.newFieldDefinition()
                              .name(field.getName())
                              .type((GraphQLOutputType) wrapNullable(fieldType, field.getType()))
                              .build()
                      )
              );
    }
    return Optional.of(builder.build());
  }

  public static String uniquifyNameForPath(NamePath fullPath) {
    return fullPath.toString("_");
  }

  public static String uniquifyNameForPath(NamePath fullPath, String postfix) {
    return fullPath.toString("_").concat(postfix);
  }
}
