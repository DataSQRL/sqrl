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
package com.datasqrl.graphql.server;

import graphql.language.ObjectTypeDefinition;
import graphql.language.SchemaDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.Optional;
import java.util.function.Supplier;

public class TypeDefinitionRegistryUtil {

  public static Optional<String> getSchemaRootTypeName(
      Optional<SchemaDefinition> schemaRoot, String schemaRootName) {
    return schemaRoot
        .flatMap(
            s ->
                s.getOperationTypeDefinitions().stream()
                    .filter(f -> f.getName().equals(schemaRootName))
                    .findFirst())
        .map(f -> f.getTypeName().getName());
  }

  public static String getTypeName(
      TypeDefinitionRegistry registry, String type, String defaultTypeName) {
    return getSchemaRootTypeName(registry.schemaDefinition(), type).orElse(defaultTypeName);
  }

  public static String getQueryTypeName(TypeDefinitionRegistry registry) {
    return getTypeName(registry, "query", "Query");
  }

  public static String getMutationTypeName(TypeDefinitionRegistry registry) {
    return getTypeName(registry, "mutation", "Mutation");
  }

  public static String getSubscriptionTypeName(TypeDefinitionRegistry registry) {
    return getTypeName(registry, "subscription", "Subscription");
  }

  public static ObjectTypeDefinition getQueryType(TypeDefinitionRegistry registry) {
    return getType(registry, () -> getQueryTypeName(registry))
        .orElseThrow(() -> new RuntimeException("Cannot find graphql Query type"));
  }

  public static Optional<ObjectTypeDefinition> getMutationType(TypeDefinitionRegistry registry) {
    return getType(registry, () -> getMutationTypeName(registry));
  }

  public static Optional<ObjectTypeDefinition> getSubscriptionType(
      TypeDefinitionRegistry registry) {
    return getType(registry, () -> getSubscriptionTypeName(registry));
  }

  public static Optional<ObjectTypeDefinition> getType(
      TypeDefinitionRegistry registry, Supplier<String> supplier) {
    var queryTypeName = supplier.get();
    return registry
        .getType(queryTypeName)
        .filter(f -> f instanceof ObjectTypeDefinition)
        .map(f -> (ObjectTypeDefinition) f);
  }
}
