package ai.datasqrl.graphql;

import static graphql.schema.FieldCoordinates.coordinates;

import ai.datasqrl.plan.local.generate.Resolve.Env;
import ai.datasqrl.schema.SQRLTable;
import graphql.language.FieldDefinition;
import graphql.language.ObjectTypeDefinition;
import graphql.language.ScalarTypeDefinition;
import graphql.language.Type;
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

    processQuery(schema, queryDef);

//    Preconditions.checkState(schema.getMutationType() == null);
//    processDirectives(schema.getSchemaDirectives());
//    processAdditionalTypes(schema.getAdditionalTypes());
//    GraphQLObjectType query = schema.getQueryType();

  }

  private void processQuery(TypeDefinitionRegistry schema, ObjectTypeDefinition queryDef) {
    for (FieldDefinition fieldDefinition : queryDef.getFieldDefinitions()) {
      Optional<SQRLTable> tableOpt = Optional.ofNullable((SQRLTable)env.getSqrlSchema()
          .getTable(fieldDefinition.getName(), false).getTable());
      SQRLTable table = tableOpt
          .orElseThrow(()->new RuntimeException("Could not find table: " +fieldDefinition));
      SchemaContext context = createContext("Query", schema, table);
      resolveRootField(fieldDefinition, context);
    }
  }

  private void resolveRootField(FieldDefinition fieldDefinition, SchemaContext context) {
    Type type = fieldDefinition.getType();
    TypeDefinition typeDef = context.schema.getType(type).get();
    System.out.println(typeDef);
  }

  private SchemaContext createContext(String typeName, TypeDefinitionRegistry schema, SQRLTable table) {
    return new SchemaContext(typeName, schema, table);
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
    TypeDefinitionRegistry schema;
    SQRLTable table;
  }
}
