package ai.dataeng.sqml;

import ai.dataeng.sqml.dag.Dag;
import ai.dataeng.sqml.execution.ExecutionResult;
import ai.dataeng.sqml.execution.LocalExecutionStrategy;
import ai.dataeng.sqml.function.FunctionProvider;
import ai.dataeng.sqml.function.PostgresFunctions;
import ai.dataeng.sqml.migrate.PostgresSqmlMigration;
import ai.dataeng.sqml.optimizer.ShreddingSqlOptimizer;
import ai.dataeng.sqml.parser.QueryParser;
import ai.dataeng.sqml.parser.SqmlParser;
import ai.dataeng.sqml.query.GraphqlQueryProvider;
import ai.dataeng.sqml.registry.LocalScriptRegistry;
import ai.dataeng.sqml.runtime.SqmlRuntime;
import ai.dataeng.sqml.schema.Schema;
import ai.dataeng.sqml.schema.SchemaField;
import ai.dataeng.sqml.schema.SchemaProvider;
import ai.dataeng.sqml.schema.Validators;
import ai.dataeng.sqml.servlet.GraphqlServlet;
import ai.dataeng.sqml.source.HttpIngress;
import ai.dataeng.sqml.source.Source;
import ai.dataeng.sqml.statistics.StaticStatisticsProvider;
import ai.dataeng.sqml.statistics.StatisticsProvider;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlType;
import ai.dataeng.sqml.type.SqmlType.StringSqmlType;
import ai.dataeng.sqml.vertex.Edge;
import ai.dataeng.sqml.vertex.PostgresViewVertexFactory;
import graphql.schema.GraphQLSchema;
import java.io.File;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;

public class Main {

  public static void main(String args[]) throws Exception {
    /* Defined as a source vertex */

    Source meetupIngres = HttpIngress.newHttpIngres()
      .url("https://stream.meetup.com/2/rsvps")
      .build();

    SqmlParser parser = SqmlParser.newSqmlParser();
    QueryParser gqlParser = QueryParser.newGraphqlParser();

    Dag dag = Dag.newDag()
      .source("meetup", meetupIngres)
      .functionProvider(
           FunctionProvider.newFunctionProvider()
             .function(PostgresFunctions.SqmlSystemFunctions))
      .scriptRegistry(
           LocalScriptRegistry.newScriptRegistry()
             .script("meetup", parser.parse(Files.readString(
                 new File(Main.class.getClassLoader().getResource("meetup.sqml").toURI()).toPath()))))
      .queryProvider(
          GraphqlQueryProvider.newQueryProvider()
            .query("meetup", gqlParser.parse("queries")))
      .statisticsProvider(
          StaticStatisticsProvider.newStatisticsProvider()
//            .addColumn(QualifiedName.of("Rsvp", "venue", "name"), new StringSqmlType())
      )
      .schemaProvider(
          SchemaProvider.newSchemaProvider()
            .addSchema("rsvp",
              Schema.newSchema("rsvp")
                .object("venue")
                .field(SchemaField.newString("venue_name"))
                .field(SchemaField.newString("lat"))
                .field(SchemaField.newString("lon"))
                .field(SchemaField.newInteger("venue_id")
                    .validator(Validators.NOT_NULL)
                    .validator(Validators.GT_ZERO))
                .allowAdditionalFields(false)
                .build()
              .buildSchema()))
      .optimizer(
          ShreddingSqlOptimizer.newOptimizer()
            .vertexFactory(
                PostgresViewVertexFactory.newSqlVertexFactory().build())
      )
      .build("meetup");
//
//    PostgresSqmlMigration sqmlMigration = PostgresSqmlMigration.newSqmlMigration()
//        .dag(dag)
//        .session(connect())
//        .build();
//
//    sqmlMigration.migrate();
//
//    SqmlRuntime runtime = SqmlRuntime.builder()
//      .dag(dag)
//      .executionStrategy(
//        LocalExecutionStrategy.newExecutionStrategy())
//      .build();
//
//    meetupIngres.getListeners().add(new Edge());
//
//    runtime.start();

//    ExecutionResult result = runtime.execute("get");

    GraphQLSchema graphqlSchema = GraphqlSchemaBuilder
        .newGraphqlSchema()
        .dag(dag)
        .build();
//
//    GraphqlServlet gqlServlet = GraphqlServlet.newGraphqlServlet()
//      .port(8080)
//      .runtime(runtime)
//      .schema(graphqlSchema)
//      .dag(dag)
//      .build();
//
//    while(true) {
//      Thread.sleep(1000);
//    }
  }

  public static Connection connect() {
    Connection c;
    try {
      Class.forName("org.postgresql.Driver");
      c = DriverManager
          .getConnection("jdbc:postgresql://localhost:5432/henneberger");
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    System.out.println("Opened database successfully");
    return c;
  }
}