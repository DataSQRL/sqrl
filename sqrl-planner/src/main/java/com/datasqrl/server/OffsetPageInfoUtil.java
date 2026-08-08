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
package com.datasqrl.server;

import static com.datasqrl.server.util.GraphqlCheckUtil.checkState;

import com.datasqrl.server.graphql.CustomScalars;
import graphql.Scalars;
import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.SourceLocation;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.GraphQLFieldDefinition;
import graphql.schema.GraphQLNonNull;
import graphql.schema.GraphQLObjectType;
import graphql.schema.GraphQLOutputType;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Opt-in pagination support: a query whose result type is a page wrapper ({@code {results:
 * [Element!] pagination: OffsetPageInfo}}) returns its rows plus pagination metadata. This util
 * detects the wrapper shape and validates the user-declared {@code OffsetPageInfo} type.
 */
public final class OffsetPageInfoUtil {

  private OffsetPageInfoUtil() {}

  public static final String PAGINATION_TYPE_NAME = "OffsetPageInfo";

  /** Canonical field -> printed type, kept in declaration order for the injected SDL. */
  private static final Map<String, String> PAGINATION_FIELDS = new LinkedHashMap<>();

  static {
    PAGINATION_FIELDS.put("pageSize", "Int!");
    PAGINATION_FIELDS.put("currentPage", "Int!");
    PAGINATION_FIELDS.put("totalRecords", "Long!");
    PAGINATION_FIELDS.put("totalPages", "Int!");
    PAGINATION_FIELDS.put("hasNextPage", "Boolean!");
    PAGINATION_FIELDS.put("hasPreviousPage", "Boolean!");
    PAGINATION_FIELDS.put("nextOffset", "Int");
    PAGINATION_FIELDS.put("prevOffset", "Int");
    PAGINATION_FIELDS.put("firstEventTime", "DateTime");
    PAGINATION_FIELDS.put("lastEventTime", "DateTime");
  }

  private static final String CANONICAL_SDL = buildCanonicalSdl();

  /** Printed type -> GraphQL type, used to build the schema type from the canonical fields. */
  private static final Map<String, GraphQLOutputType> GRAPHQL_TYPES =
      Map.of(
          "Int!",
          GraphQLNonNull.nonNull(Scalars.GraphQLInt),
          "Long!",
          GraphQLNonNull.nonNull(CustomScalars.LONG),
          "Boolean!",
          GraphQLNonNull.nonNull(Scalars.GraphQLBoolean),
          "Int",
          Scalars.GraphQLInt,
          "DateTime",
          CustomScalars.FLEXIBLE_DATETIME);

  /** Builds the canonical {@code OffsetPageInfo} object type for generated schemas. */
  public static GraphQLObjectType createPageInfoType() {
    var builder = GraphQLObjectType.newObject().name(PAGINATION_TYPE_NAME);
    PAGINATION_FIELDS.forEach(
        (name, type) ->
            builder.field(
                GraphQLFieldDefinition.newFieldDefinition()
                    .name(name)
                    .type(GRAPHQL_TYPES.get(type))));
    return builder.build();
  }

  private static String buildCanonicalSdl() {
    var sb = new StringBuilder("type ").append(PAGINATION_TYPE_NAME).append(" {\n");
    PAGINATION_FIELDS.forEach(
        (name, type) -> sb.append("  ").append(name).append(": ").append(type).append("\n"));
    return sb.append("}\n").toString();
  }

  /**
   * Validates that a paginated query's schema declares the {@code OffsetPageInfo} type and that it
   * matches the canonical definition. Users must declare the type themselves; we only validate it.
   * Must be called from within the schema validator so errors are reported like other schema
   * errors.
   */
  public static void validatePaginationType(
      TypeDefinitionRegistry registry, SourceLocation location) {
    var existing = registry.getType(PAGINATION_TYPE_NAME);
    checkState(
        existing.isPresent(),
        location,
        "Paginated results require the %s type to be declared in the schema:\n%s",
        PAGINATION_TYPE_NAME,
        CANONICAL_SDL);
    checkState(
        existing.get() instanceof ObjectTypeDefinition,
        existing.get().getSourceLocation(),
        "%s must be an object type",
        PAGINATION_TYPE_NAME);
    validateMatchesCanonical((ObjectTypeDefinition) existing.get());
  }

  /**
   * Detects the page wrapper shape: an object type with exactly two fields, one a list of an object
   * type (the results) and the other of type {@code OffsetPageInfo}. Returns the element object
   * type when the shape matches.
   */
  public static Optional<ObjectTypeDefinition> getPagedElementType(
      ObjectTypeDefinition wrapper, TypeDefinitionRegistry registry) {
    var fields = wrapper.getFieldDefinitions();
    if (fields.size() != 2) {
      return Optional.empty();
    }

    ObjectTypeDefinition elementType = null;
    boolean hasPagination = false;
    for (FieldDefinition field : fields) {
      var type = unwrapNonNull(field.getType());
      if (type instanceof ListType listType) {
        var element = unwrapNonNull(listType.getType());
        if (element instanceof TypeName typeName) {
          elementType =
              registry
                  .getType(typeName.getName())
                  .filter(t -> t instanceof ObjectTypeDefinition)
                  .map(t -> (ObjectTypeDefinition) t)
                  .orElse(null);
        }
      } else if (type instanceof TypeName typeName
          && PAGINATION_TYPE_NAME.equals(typeName.getName())) {
        hasPagination = true;
      }
    }

    return hasPagination ? Optional.ofNullable(elementType) : Optional.empty();
  }

  private static void validateMatchesCanonical(ObjectTypeDefinition userType) {
    var actual = new LinkedHashMap<String, String>();
    for (FieldDefinition field : userType.getFieldDefinitions()) {
      actual.put(field.getName(), printType(field.getType()));
    }
    checkState(
        actual.equals(PAGINATION_FIELDS),
        userType.getSourceLocation(),
        "User-defined %s does not match the expected definition:\n%s",
        PAGINATION_TYPE_NAME,
        CANONICAL_SDL);
  }

  private static String printType(Type<?> type) {
    if (type instanceof NonNullType nonNull) {
      return printType(nonNull.getType()) + "!";
    }
    if (type instanceof ListType listType) {
      return "[" + printType(listType.getType()) + "]";
    }
    if (type instanceof TypeName typeName) {
      return typeName.getName();
    }
    return type.toString();
  }

  private static Type<?> unwrapNonNull(Type<?> type) {
    return type instanceof NonNullType nonNull ? unwrapNonNull(nonNull.getType()) : type;
  }
}
