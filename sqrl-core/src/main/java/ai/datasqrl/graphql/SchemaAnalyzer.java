package ai.datasqrl.graphql;

import static graphql.schema.FieldCoordinates.coordinates;

import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.SQRLTable;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.TypeDefinition;
import graphql.schema.DataFetcher;
import graphql.schema.GraphQLCodeRegistry;
import graphql.schema.GraphQLScalarType;
import graphql.schema.GraphQLSchema;
import graphql.schema.GraphQLSchema.Builder;
import graphql.schema.idl.ScalarInfo;
import graphql.schema.idl.TypeDefinitionRegistry;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import lombok.Value;

public class SchemaAnalyzer {

  public SchemaAnalyzer(Env env) {
    this.env = env;
  }

  @Value
  class Analysis {
    Set<GraphQLScalarType> additionalTypes = new HashSet<>();
//    Map<FieldCoordinates, DataFetcher> fetchers = new HashMap<>();
  }
  Builder schemaBuilder = new GraphQLSchema.Builder();
  Analysis analysis = null;

  Env env;

  public void walkSchema(TypeDefinitionRegistry schema) {
    Optional<TypeDefinition> query = schema.getType("Query");
    ObjectTypeDefinition queryDef = (ObjectTypeDefinition) query.get();

    processScalars(schema.scalars());

    processQuery(queryDef);
    System.out.printf("");

//    Preconditions.checkState(schema.getMutationType() == null);
//    processDirectives(schema.getSchemaDirectives());
//    processAdditionalTypes(schema.getAdditionalTypes());
//    GraphQLObjectType query = schema.getQueryType();

  }

  private void processQuery(ObjectTypeDefinition queryDef) {
    for (FieldDefinition fieldDefinition : queryDef.getFieldDefinitions()) {
      SQRLTable table = (SQRLTable)env.getSqrlSchema().plus().getTable(queryDef.getName());
      SchemaContext context = createContext("Query", table);
      resolveField(fieldDefinition, context);
    }
  }

  private void resolveField(FieldDefinition fieldDefinition, SchemaContext context) {
    System.out.println(fieldDefinition);
  }

  private SchemaContext createContext(String typeName, SQRLTable table) {
    return new SchemaContext(typeName, table);
  }

  private void processScalars(Map<String, ScalarTypeDefinition> scalars) {
    for (Map.Entry<String, ScalarTypeDefinition> type : scalars.entrySet()) {
      processAdditionalType(type.getKey(), type.getValue());
    }
  }

  private void processAdditionalType(String key, ScalarTypeDefinition value) {
    AdditionalTypeResolver typeResolver = new AdditionalTypeResolver();
    GraphQLScalarType scalar = typeResolver.resolve(value);
    //Ignore built in types
    if (!ScalarInfo.GRAPHQL_SPECIFICATION_SCALARS_DEFINITIONS.containsKey(key)) {
      schemaBuilder.additionalType(scalar);
    }
  }

  class QueryDataFetcher {
    GraphQLCodeRegistry codeRegistry = GraphQLCodeRegistry.newCodeRegistry()
        .dataFetcher(
            coordinates("ObjectType", "description"),
            (DataFetcher<Object[]>) dataFetchingEnvironment -> {
              return null;
            })
//            PropertyDataFetcher.fetching("desc"))
        .build();

  }

  @Value
  private class SchemaContext {
    String typeName;
    SQRLTable table;
  }
}
