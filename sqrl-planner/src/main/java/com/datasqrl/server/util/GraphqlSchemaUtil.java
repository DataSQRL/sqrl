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
package com.datasqrl.server.util;

import static com.datasqrl.canonicalizer.Name.isSystemHidden;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.plan.table.Multiplicity;
import com.datasqrl.planner.util.Documented.Lookup;
import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.Type;
import graphql.schema.GraphQLInputType;
import graphql.schema.GraphQLList;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLOutputType;
import graphql.schema.GraphQLType;
import java.util.Optional;
import java.util.regex.Pattern;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;

@Slf4j
public class GraphqlSchemaUtil {

  public static GraphQLType wrapNullable(GraphQLType gqlType, RelDataType type) {
    if (!type.isNullable()) {
      return GraphQLNonNull.nonNull(gqlType);
    }
    return gqlType;
  }

  public static GraphQLType wrapMultiplicity(GraphQLType type, Multiplicity multiplicity) {
    return switch (multiplicity) {
      case ZERO_ONE -> type;
      case ONE -> GraphQLNonNull.nonNull(type);
      case MANY -> GraphQLList.list(GraphQLNonNull.nonNull(type));
    };
  }

  /** Checks if the graphql schema has a different case than the graphql schema */
  public static boolean hasVaryingCase(FieldDefinition field, RelDataTypeField relDataTypeField) {
    return !field.getName().equals(relDataTypeField.getName());
  }

  public static boolean isValidGraphQLName(String name) {
    return !isSystemHidden(name) && Pattern.matches("[_A-Za-z][_0-9A-Za-z]*", name);
  }

  public static boolean isListType(Type type) {
    if (type instanceof NonNullType) {
      type = ((NonNullType) type).getType();
    }
    return type instanceof ListType;
  }

  public static GraphQLType asList(GraphQLType type) {
    return GraphQLNonNull.nonNull(GraphQLList.list(type));
  }

  public static GraphQLInputType asInputType(GraphQLType type) {
    return (GraphQLInputType) type;
  }

  public static GraphQLOutputType asOutputType(GraphQLType type) {
    return (GraphQLOutputType) type;
  }

  public static Optional<GraphQLInputType> getGraphQLInputType(
      RelDataType type, NamePath namePath, boolean extendedScalarTypes, Lookup fieldDocs) {
    return getGraphQLType(GraphQLMetaType.INPUT, type, namePath, extendedScalarTypes, fieldDocs)
        .map(inputType -> wrapNullable(inputType, type))
        .map(GraphqlSchemaUtil::asInputType);
  }

  public static Optional<GraphQLOutputType> getGraphQLOutputType(
      RelDataType type, NamePath namePath, boolean extendedScalarTypes, Lookup fieldDocs) {
    return getGraphQLType(GraphQLMetaType.OUTPUT, type, namePath, extendedScalarTypes, fieldDocs)
        .map(t -> wrapNullable(t, type))
        .map(GraphqlSchemaUtil::asOutputType);
  }

  /**
   * Maps a type to its GraphQL type, without wrapping it in {@code NonNull}.
   *
   * @param namePath the path of the type, which names the GraphQL types of nested rows
   * @param fieldDocs the docs of the fields of the type, if it is a row
   * @return the GraphQL type, empty if the type has no GraphQL type
   */
  public static Optional<GraphQLType> getGraphQLType(
      GraphQLMetaType metaType,
      RelDataType type,
      NamePath namePath,
      boolean extendedScalarTypes,
      Lookup fieldDocs) {
    return new GraphqlTypeWalker(metaType, extendedScalarTypes, namePath, fieldDocs)
        .walk(type, namePath);
  }

  @AllArgsConstructor
  public enum GraphQLMetaType {
    INPUT("Input"),
    OUTPUT("Output");

    @Getter private final String suffix;
  }

  /**
   * Create a unique type name from a NamePath by concatenating the display names separated by
   * underscore (_)
   *
   * @param fullPath
   * @return
   */
  public static String uniquifyNameForPath(NamePath fullPath) {
    return fullPath.toString("_");
  }

  public static String uniquifyNameForPath(NamePath fullPath, String postfix) {
    return uniquifyNameForPath(fullPath).concat(postfix);
  }
}
