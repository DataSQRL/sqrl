package ai.dataeng.sqml.analyzer2;

import graphql.ExecutionInput;
import graphql.ExecutionResult;
import graphql.GraphQL;
import graphql.GraphQLError;
import graphql.schema.GraphQLSchema;
import io.vertx.core.Vertx;
import io.vertx.core.impl.VertxInternal;
import java.util.List;

public class GraphqlBuilder {


  public static GraphQL graphqlTest(Vertx vertx, GraphQLSchema schema) throws Exception {

    ///Graphql bit

//    SchemaParser schemaParser = new SchemaParser();
//    URL url = com.google.common.io.Resources.getResource("c360-small.graphqls");
//    String schema = com.google.common.io.Resources.toString(url, Charsets.UTF_8);
//    TypeDefinitionRegistry typeDefinitionRegistry = schemaParser.parse(schema);
//
//    graphql.schema.idl.SchemaGenerator schemaGenerator = new graphql.schema.idl.SchemaGenerator();




//    System.out.println(new SchemaPrinter().print(graphQLSchema));

    GraphQL graphQL = GraphQL.newGraphQL(schema)
        .build();

    return graphQL;
  }
}
