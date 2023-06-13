package com.datasqrl.graphql.inference;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.graphql.APIConnectorManager;
import com.datasqrl.graphql.visitor.GraphqlSchemaVisitor;
import com.datasqrl.plan.queries.APIMutation;
import com.datasqrl.plan.queries.APIConnectors;
import com.datasqrl.plan.queries.APISource;
import com.datasqrl.plan.queries.APISubscription;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import graphql.language.ObjectTypeDefinition;
import graphql.schema.idl.SchemaParser;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.type.RelDataTypeFactory;

@Slf4j
@Singleton
public class GraphQLMutationExtraction {

  private final RelDataTypeFactory typeFactory;
  @Getter
  private final NameCanonicalizer canonicalizer;

  @Inject
  public GraphQLMutationExtraction(RelDataTypeFactory typeFactory, NameCanonicalizer nameCanonicalizer) {
    this.typeFactory = typeFactory;
    this.canonicalizer = nameCanonicalizer;
  }

  public void analyze(APISource apiSource, APIConnectorManager apiManager) {
    TypeDefinitionRegistry registry = (new SchemaParser()).parse(apiSource.getSchemaDefinition());
    ObjectTypeDefinition mutationType = (ObjectTypeDefinition) registry
        .getType("Mutation")
        .orElse(null);
    if (mutationType == null) {
      log.trace("No mutations in {}", apiSource);
    } else {
      GraphqlSchemaVisitor.accept(new InputFieldToFlexibleSchemaRelation(registry, typeFactory, canonicalizer),
              mutationType, null)
          .forEach(utb -> apiManager.addMutation(new APIMutation(utb.getName(), apiSource, utb)));
    }

//    List<graphql.language.FieldDefinition> subscriptions = registry
//        .getType("Subscription")
//        .map(sub -> ((ObjectTypeDefinition)sub).getFieldDefinitions())
//        .orElse(List.of());
//    if (subscriptions.isEmpty()) log.trace("No subscriptions in {}", apiSource);
//    for (graphql.language.FieldDefinition definition : subscriptions) {
//      builder.subscription(new APISubscription(canonicalizer.name(definition.getName()), apiSource));
//    }
//    return builder.build();
  }

}
