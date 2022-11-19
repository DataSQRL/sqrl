package ai.datasqrl;

import ai.datasqrl.graphql.GraphQLServer;
import ai.datasqrl.graphql.inference.AbstractSchemaInferenceModelTest;
import ai.datasqrl.graphql.server.Model;
import ai.datasqrl.graphql.util.ReplaceGraphqlQueries;
import ai.datasqrl.physical.PhysicalPlan;
import ai.datasqrl.physical.stream.Job;
import ai.datasqrl.physical.stream.PhysicalPlanExecutor;
import ai.datasqrl.plan.global.DAGPlanner;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.local.generate.Resolve;
import ai.datasqrl.plan.queries.APIQuery;
import io.vertx.core.Vertx;
import io.vertx.ext.web.client.WebClient;
import io.vertx.ext.web.client.WebClientOptions;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import lombok.SneakyThrows;
import org.apache.calcite.sql.ScriptNode;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@ExtendWith(VertxExtension.class)
public class AbstractQuerySQRLIT extends AbstractPhysicalSQRLIT {

    protected Vertx vertx;
    protected VertxTestContext vertxContext;

    @SneakyThrows
    protected void validateSchemaAndQueries(String script, String schema, Map<String,String> queries) {
        ScriptNode node = parse(script);
        Resolve.Env resolvedDag = resolve.planDag(session, node);
        DAGPlanner dagPlanner = new DAGPlanner(planner);

        Pair<Model.Root, List<APIQuery>> modelAndQueries = AbstractSchemaInferenceModelTest.getModelAndQueries(resolvedDag,schema);

        OptimizedDAG dag = dagPlanner.plan(resolvedDag.getRelSchema(), modelAndQueries.getRight(),
                resolvedDag.getExports(), session.getPipeline());

        PhysicalPlan physicalPlan = physicalPlanner.plan(dag);

        Model.Root model = modelAndQueries.getKey();
        ReplaceGraphqlQueries replaceGraphqlQueries = new ReplaceGraphqlQueries(physicalPlan.getDatabaseQueries());
        model.accept(replaceGraphqlQueries, null);
        snapshot.addContent(physicalPlan.getDatabaseDDL().stream().map(ddl -> ddl.toSql())
                .sorted().collect(Collectors.joining(System.lineSeparator())),"database");

        PhysicalPlanExecutor executor = new PhysicalPlanExecutor();
        Job job = executor.execute(physicalPlan);
        System.out.println("Started Flink Job: " + job.getExecutionId());

        Checkpoint serverStarted = vertxContext.checkpoint();
        Checkpoint queryResponse = vertxContext.checkpoint(queries.size());
        Map<String, String> queryResults = new ConcurrentHashMap<>();
        vertx.deployVerticle(new GraphQLServer(null, model, jdbc), vertxContext.succeeding(server -> {
            serverStarted.flag();
            WebClient client = getGraphQLClient();
            for (Map.Entry<String,String> query : queries.entrySet()) {
                processGraphQLQuery(client, query.getValue(), result -> {
                    queryResults.put(query.getKey(),result);
                    queryResponse.flag();
                });
            }
        }));

        vertxContext.awaitCompletion(5, TimeUnit.SECONDS);
        //We process the results after completion to make sure the order of the queries is preserved for comparability
        for (Map.Entry<String,String> query : queries.entrySet()) {
            snapshot.addContent(queryResults.get(query.getKey()), "query-" + query.getKey());
        }
        snapshot.createOrValidate();
    }

    protected WebClient getGraphQLClient() {
        WebClientOptions options = new WebClientOptions()
                .setUserAgent(WebClientOptions.loadUserAgent())
                .setDefaultHost("localhost")
                .setDefaultPort(8888);
        return WebClient.create(vertx,options);
    }

    protected void processGraphQLQuery(WebClient client, String query, Consumer<String> resultHandler) {
        client.post("/graphql")
                        .sendJson(Map.of("query", query))
                .onComplete(vertxContext.succeeding(data -> vertxContext.verify(() -> {
                    Assert.assertNotNull(data);
                    resultHandler.accept(data.bodyAsString());
                })));
    }
}
