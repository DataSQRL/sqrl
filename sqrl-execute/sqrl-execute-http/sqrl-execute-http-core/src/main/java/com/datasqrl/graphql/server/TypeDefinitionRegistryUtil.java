package com.datasqrl.graphql.server;

import graphql.language.SchemaDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.Optional;

public class TypeDefinitionRegistryUtil {

  public static Optional<String> getSchemaRootTypeName(Optional<SchemaDefinition> schemaRoot, String schemaRootName) {
    return schemaRoot.flatMap(s->s.getOperationTypeDefinitions().stream()
            .filter(f->f.getName().equals(schemaRootName))
            .findFirst())
        .map(f->f.getTypeName().getName());
  }

  public static String getTypeName(TypeDefinitionRegistry registry, String type, String defaultTypeName) {
    return getSchemaRootTypeName(registry.schemaDefinition(), type)
        .orElse(defaultTypeName);
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
}
