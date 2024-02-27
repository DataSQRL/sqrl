package com.datasqrl.graphql.server;

import graphql.language.ObjectTypeDefinition;
import graphql.language.SchemaDefinition;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.regex.Pattern;

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

  public static ObjectTypeDefinition getQueryType(TypeDefinitionRegistry registry) {
    return getType(registry, ()->getQueryTypeName(registry))
        .orElseThrow(()->new RuntimeException("Cannot find graphql Query type"));
  }

  public static Optional<ObjectTypeDefinition> getMutationType(TypeDefinitionRegistry registry) {
    return getType(registry, ()->getMutationTypeName(registry));
  }

  public static Optional<ObjectTypeDefinition> getSubscriptionType(TypeDefinitionRegistry registry) {
    return getType(registry, ()->getSubscriptionTypeName(registry));
  }

  public static Optional<ObjectTypeDefinition> getType(TypeDefinitionRegistry registry, Supplier<String> supplier) {
    String queryTypeName = supplier.get();
    return registry.getType(queryTypeName)
        .filter(f->f instanceof ObjectTypeDefinition)
        .map(f->(ObjectTypeDefinition)f);
  }
}
