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


  public static void graphqlTest(Vertx vertx, GraphQLSchema schema) throws Exception {

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

    ExecutionInput executionInput = ExecutionInput.newExecutionInput().query(
            "query Test {\n"
                + "    customerorderstats { customerid, num_orders }\n"
                + "    orders {"//(limit: 2) {\n"
                + "        customerid, id\n"
                + "        entries {"//(order_by: [{discount: DESC}]) {\n"
//                + "            data {\n"
                + "               total, discount\n"
//                + "            } \n"
//                + "            pageInfo { \n"
//                + "                cursor\n"
//                + "                hasNext\n"
//                + "             }\n"
                + "        }\n"
                + "    }\n"
                + "}")
//        .dataLoaderRegistry(dataLoaderRegistry)
        .build();
    ExecutionResult executionResult = graphQL.execute(executionInput);

    Object data = executionResult.getData();
    System.out.println();
    System.out.println(data);
    List<GraphQLError> errors2 = executionResult.getErrors();
    System.out.println(errors2);

    vertx.close();
  }
}
