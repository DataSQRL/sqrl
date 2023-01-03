/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.graphql.generate;

import com.datasqrl.schema.Field;
import com.datasqrl.schema.Multiplicity;
import com.datasqrl.schema.Relationship;
import com.datasqrl.schema.SQRLTable;
import com.google.common.collect.BiMap;
import graphql.Scalars;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import graphql.schema.GraphQLTypeReference;
import org.apache.calcite.rel.type.RelDataType;

public class SchemaGeneratorUtil {

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
    String conformed = conformName(table.getName().getDisplay());

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

  public static GraphQLInputType getInputType(RelDataType type) {
    return (GraphQLInputType) getInOutType(type);
  }

  public static GraphQLOutputType getOutputType(RelDataType type) {
    return (GraphQLOutputType) getInOutType(type);
  }

  public static GraphQLType getInOutType(RelDataType type) {
    return getInOutTypeHelper(type);
  }

  public static GraphQLType getInOutTypeHelper(RelDataType type) {
    switch (type.getSqlTypeName()) {
      case BOOLEAN:
        return Scalars.GraphQLBoolean;
      case TINYINT:
      case SMALLINT:
      case INTEGER:
      case BIGINT:
        return Scalars.GraphQLInt;
      case DECIMAL:
      case FLOAT:
      case REAL:
      case DOUBLE:
        return Scalars.GraphQLFloat;
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
        return Scalars.GraphQLString;
      case ARRAY:
      case MULTISET:
        return GraphQLList.list(getOutputType(type.getComponentType()));
      case STRUCTURED:
      case ROW:
//        String name = uniqueName("struct");
//        ObjectTypeBuilder objectTypeBuilder = new ObjectTypeBuilder(name);
//        objectTypeBuilderList.add(objectTypeBuilder);
//        objectTypeBuilder.createStructField(type.getComponentType());
//        return new GraphQLTypeReference(name);
      case BINARY:
      case VARBINARY:
      case NULL:
      case ANY:
      case SYMBOL:
      case DISTINCT:
      case MAP:
      case OTHER:
      case CURSOR:
      case COLUMN_LIST:
      case DYNAMIC_STAR:
      case GEOMETRY:
      case SARG:
      default:
        throw new RuntimeException("Unknown graphql schema type");
    }
  }

  public static boolean isAccessible(SQRLTable table) {
    return !table.getName().isHidden();
  }

  public static boolean isAccessible(Field field) {
    if (field instanceof Relationship && !isAccessible(((Relationship)field).getToTable())) {
      return false;
    }

    return !field.getName().isHidden();
  }
}
