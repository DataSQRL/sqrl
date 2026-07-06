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

import graphql.language.FieldDefinition;
import graphql.language.ListType;
import graphql.language.NonNullType;
import graphql.language.ObjectTypeDefinition;
import graphql.language.Type;
import graphql.language.TypeName;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Opt-in pagination support: a query whose result type is a page wrapper ({@code {results:
 * [Element!] pagination: SqrlPagination}}) returns its rows plus pagination metadata computed from
 * a companion COUNT(*) query. This util detects the wrapper shape and injects/validates the
 * standard {@code SqrlPagination} type.
 */
public final class SqrlPaginationUtil {

  private SqrlPaginationUtil() {}

  public static final String PAGINATION_TYPE_NAME = "SqrlPagination";

  /** Canonical field -> printed type, kept in declaration order for the injected SDL. */
  private static final Map<String, String> PAGINATION_FIELDS = new LinkedHashMap<>();

  static {
    PAGINATION_FIELDS.put("totalRecords", "Long!");
    PAGINATION_FIELDS.put("pageSize", "Int!");
    PAGINATION_FIELDS.put("currentPage", "Int!");
    PAGINATION_FIELDS.put("totalPages", "Int!");
    PAGINATION_FIELDS.put("hasNextPage", "Boolean!");
    PAGINATION_FIELDS.put("hasPreviousPage", "Boolean!");
    PAGINATION_FIELDS.put("nextOffset", "Int");
    PAGINATION_FIELDS.put("prevOffset", "Int");
    PAGINATION_FIELDS.put("firstEventTime", "DateTime");
    PAGINATION_FIELDS.put("lastEventTime", "DateTime");
  }

  private static final String CANONICAL_SDL = buildCanonicalSdl();

  private static String buildCanonicalSdl() {
    var sb = new StringBuilder("type ").append(PAGINATION_TYPE_NAME).append(" {\n");
    PAGINATION_FIELDS.forEach(
        (name, type) -> sb.append("  ").append(name).append(": ").append(type).append("\n"));
    return sb.append("}\n").toString();
  }

  /**
   * If the schema references {@code SqrlPagination} but does not define it, append the canonical
   * definition (plus any missing scalar declarations). A user-provided definition is left untouched
   * here and validated later by {@link #validatePaginationType} within the schema validator's error
   * scope. Returns the (possibly rewritten) source.
   */
  public static ApiSource injectPaginationType(ApiSource schema) {
    TypeDefinitionRegistry registry;
    try {
      registry = new SchemaParser().parse(schema.getDefinition());
    } catch (Exception e) {
      // Let the downstream validator report parse errors with proper source location; an
      // unparseable schema cannot reference the pagination type anyway.
      return schema;
    }

    if (!referencesPaginationType(registry) || registry.getType(PAGINATION_TYPE_NAME).isPresent()) {
      return schema;
    }

    var injected = new StringBuilder(schema.getDefinition());
    injected.append("\n");
    if (registry.scalars().get("Long") == null) {
      injected.append("scalar Long\n");
    }
    if (registry.scalars().get("DateTime") == null) {
      injected.append("scalar DateTime\n");
    }
    injected.append(CANONICAL_SDL);

    return new ApiSource(schema.getPath().orElse(null), injected.toString());
  }

  /**
   * Validates that a user-provided {@code SqrlPagination} type matches the canonical definition.
   * Must be called from within the schema validator so mismatches are reported like other schema
   * errors. No-op when the type is absent (it will have been injected) or unreferenced.
   */
  public static void validatePaginationType(TypeDefinitionRegistry registry) {
    var existing = registry.getType(PAGINATION_TYPE_NAME);
    if (existing.isEmpty()) {
      return;
    }
    checkState(
        existing.get() instanceof ObjectTypeDefinition,
        existing.get().getSourceLocation(),
        "%s must be an object type",
        PAGINATION_TYPE_NAME);
    validateMatchesCanonical((ObjectTypeDefinition) existing.get());
  }

  /**
   * Detects the page wrapper shape: an object type with exactly two fields, one a list of an object
   * type (the results) and the other of type {@code SqrlPagination}. Returns the element object
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

  private static boolean referencesPaginationType(TypeDefinitionRegistry registry) {
    return registry.types().values().stream()
        .filter(t -> t instanceof ObjectTypeDefinition)
        .flatMap(t -> ((ObjectTypeDefinition) t).getFieldDefinitions().stream())
        .anyMatch(field -> referencesPaginationType(field.getType()));
  }

  private static boolean referencesPaginationType(Type<?> type) {
    var unwrapped = unwrapNonNull(type);
    if (unwrapped instanceof ListType listType) {
      return referencesPaginationType(listType.getType());
    }
    return unwrapped instanceof TypeName typeName
        && PAGINATION_TYPE_NAME.equals(typeName.getName());
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
