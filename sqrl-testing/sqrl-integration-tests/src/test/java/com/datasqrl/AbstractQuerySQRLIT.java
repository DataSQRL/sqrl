///*
// * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
// */
//package com.datasqrl;
//
//import com.datasqrl.actions.GraphqlPostplanHook;
//import com.datasqrl.calcite.SqrlFramework;
//import com.datasqrl.canonicalizer.Name;
//import com.datasqrl.canonicalizer.NameCanonicalizer;
//import com.datasqrl.engine.PhysicalPlan;
//import com.datasqrl.engine.PhysicalPlanExecutor;
//import com.datasqrl.engine.PhysicalPlanner;
//import com.datasqrl.engine.database.relational.JDBCPhysicalPlan;
//import com.datasqrl.engine.pipeline.ExecutionPipeline;
//import com.datasqrl.engine.server.GenericJavaServerEngine;
//import com.datasqrl.error.ErrorCollector;
//import com.datasqrl.graphql.APIConnectorManager;
//import com.datasqrl.graphql.GraphQLServer;
//import com.datasqrl.graphql.config.ServerConfig;
//import com.datasqrl.graphql.inference.AbstractSchemaInferenceModelTest;
//import com.datasqrl.graphql.inference.GraphqlModelGenerator;
//import com.datasqrl.graphql.server.RootGraphqlModel.Coords;
//import com.datasqrl.graphql.server.RootGraphqlModel;
//import com.datasqrl.graphql.server.RootGraphqlModel.StringSchema;
//import com.datasqrl.plan.global.DAGAssembler;
//import com.datasqrl.plan.global.DAGBuilder;
//import com.datasqrl.plan.global.DAGPlanner;
//import com.datasqrl.plan.global.DAGPreparation;
//import com.datasqrl.plan.global.PhysicalDAGPlan;
//import com.datasqrl.plan.queries.APISource;
//import com.datasqrl.plan.queries.APISourceImpl;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import graphql.schema.idl.SchemaParser;
//import io.vertx.core.Vertx;
//import io.vertx.core.json.JsonObject;
//import io.vertx.junit5.VertxExtension;
//import io.vertx.junit5.VertxTestContext;
//import java.net.ServerSocket;
//import java.net.URI;
//import java.net.http.HttpClient;
//import java.net.http.HttpRequest;
//import java.net.http.HttpResponse;
//import java.net.http.HttpResponse.BodyHandlers;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.CountDownLatch;
//import java.util.concurrent.TimeUnit;
//import java.util.stream.Collectors;
//import lombok.SneakyThrows;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.commons.lang3.tuple.Triple;
//import org.junit.jupiter.api.AfterEach;
//import org.junit.jupiter.api.extension.ExtendWith;
//
//@ExtendWith(VertxExtension.class)
//public class AbstractQuerySQRLIT extends AbstractPhysicalSQRLIT {
//
//  protected Vertx vertx;
//  protected VertxTestContext vertxContext;
//
//  ObjectMapper mapper = new ObjectMapper();
//  private int port;
//
//  @SneakyThrows
//  @AfterEach
//  public void stop() {
//    if (vertx != null) {
//      vertx.close().toCompletionStage().toCompletableFuture().get();
//    }
//  }
//
//  @SneakyThrows
//  protected void validateSchemaAndQueries(String script, String schema, Map<String, String> queries) {
//    plan(script);
//    inferSchemaModelQueries(schema, framework, errors);
//    APIConnectorManager apiManager = injector.getInstance(APIConnectorManager.class);
//    PhysicalDAGPlan dag = injector.getInstance(DAGPlanner.class).plan();
//    PhysicalPlan physicalPlan = injector.getInstance(PhysicalPlanner.class)
//        .plan(dag);
//    APISource source = new APISourceImpl(Name.system("<schema>"), schema);
//
//    GraphqlModelGenerator queryGenerator = new GraphqlModelGenerator(framework.getCatalogReader().nameMatcher(),
//        framework.getSchema(),
//        physicalPlan.getDatabaseQueries(), framework.getQueryPlanner(), apiManager);
//
//    queryGenerator.walk(source);
//
//    RootGraphqlModel model = RootGraphqlModel.builder()
//        .schema(StringSchema.builder().schema(source.getSchemaDefinition()).build())
//        .coords(queryGenerator.getCoords())
//        .mutations(queryGenerator.getMutations())
//        .subscriptions(queryGenerator.getSubscriptions())
//        .build();
//
//    snapshot.addContent(
//        physicalPlan.getPlans(JDBCPhysicalPlan.class).findFirst().get().getDdlStatements().stream()
//            .map(ddl -> ddl.toSql())
//            .sorted().collect(Collectors.joining(System.lineSeparator())), "database");
//
//    PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
//    PhysicalPlanExecutor.Result result = executor.execute(physicalPlan, errors);
//    CompletableFuture[] completableFutures = result.getResults().stream()
//        .map(s->s.getResult())
//        .toArray(CompletableFuture[]::new);
//    CompletableFuture.allOf(completableFutures)
//        .get();
//
//    CountDownLatch countDownLatch = new CountDownLatch(1);
//
//    ServerConfig serverConfig = new ServerConfig(new JsonObject());
//    this.port = getPort(8888);
//    GenericJavaServerEngine.applyDefaults(serverConfig, jdbc.get(), this.port);
//
//    GraphQLServer server = new GraphQLServer(model, serverConfig, NameCanonicalizer.SYSTEM);
//    vertx.deployVerticle(server, c->countDownLatch.countDown());
//    countDownLatch.await(10, TimeUnit.SECONDS);
//    if (countDownLatch.getCount() != 0) {
//      throw new RuntimeException("Could not start vertx");
//    }
//
//    for (Map.Entry<String, String> query : queries.entrySet()) {
//      HttpResponse<String> response = testQuery(query.getValue());
//      String httpQuery = prettyPrint(response.body());
//
//      snapshot.addContent(httpQuery, "query", query.getKey());
//    }
//    snapshot.createOrValidate();
//    vertxContext.completeNow();
//  }
//
//  @SneakyThrows
//  private static int getPort(int port) {
//    int attempts = 10;
//    while(attempts-- != 0) {
//      try (ServerSocket serverSocket = new ServerSocket(port)) {
//        boolean isPortBound = serverSocket.isBound();
//        if (isPortBound) {
//          return port;
//        }
//      } catch (Exception e) {
//        port++;
//      }
//    }
//    return -1;
//  }
//
//  @SneakyThrows
//  private String prettyPrint(String body) {
//    return mapper.writerWithDefaultPrettyPrinter()
//        .writeValueAsString(mapper.readTree(body));
//  }
//
//  @SneakyThrows
//  public HttpResponse<String> testQuery(String query) {
//    HttpClient client = HttpClient.newHttpClient();
//    HttpRequest request = HttpRequest.newBuilder()
//        .POST(HttpRequest.BodyPublishers.ofString(mapper
//            .writeValueAsString(
//                Map.of("query", query))))
//        .uri(URI.create("http://localhost:" + port + "/graphql"))
//        .build();
//    return client.send(request, BodyHandlers.ofString());
//  }
//}
