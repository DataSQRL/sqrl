package ai.dataeng.sqml.config.server;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.type.basic.ProcessMessage;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class ApiVerticle extends AbstractVerticle {

    private HttpServer server;

    private final Environment environment;

    public ApiVerticle(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        RouterBuilder.create(this.vertx, "datasqrl-openapi.yml")
                .onSuccess(routerBuilder -> {
                    // Add routes handlers
                    List<JsonObject> sources = environment.getDatasetRegistry().getDatasets().stream()
                            .map(ApiVerticle::source2Json).collect(Collectors.toList());
                    routerBuilder.operation("getSources").handler(routingContext ->
                            routingContext
                                    .response()
                                    .setStatusCode(200)
                                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                                    .end(new JsonArray(sources).encode())
                    );
                    routerBuilder.operation("addFileSource").handler(new SourceOperationHandler<>(
                            FileSourceConfiguration.class, Operation.ADD));
                    routerBuilder.operation("updateFileSource").handler(new SourceOperationHandler<>(
                            FileSourceConfiguration.class, Operation.UPDATE));
                    routerBuilder.operation("getSourceByName").handler(routingContext -> {
                        RequestParameters params = routingContext.get("parsedParameters");
                        String sourceName = params.pathParameter("sourceName").getString();
                        SourceDataset ds = environment.getDatasetRegistry().getDataset(sourceName);
                        if (ds!=null) {
                            JsonObject result = source2Json(ds);
                            routingContext
                                    .response()
                                    .setStatusCode(200)
                                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                                    .end(result.encode());
                        } else {
                            routingContext.fail(404, new Exception("Source not found"));
                        }
                    });
                    routerBuilder.operation("deleteSource").handler(routingContext -> {
                        RequestParameters params = routingContext.get("parsedParameters");
                        String sourceName = params.pathParameter("sourceName").getString();
//                        if (source.isPresent())
//                            routingContext
//                                    .response()
//                                    .setStatusCode(200)
//                                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
//                                    .end();
//                        else
                            routingContext.fail(404, new Exception("Not yet implemented")); // <5>
                    });

                    // Generate the router
                    // tag::routerGen[]
                    Router router = routerBuilder.createRouter(); // <1>
                    router.errorHandler(404, routingContext -> { // <2>
                        JsonObject errorObject = new JsonObject() // <3>
                                .put("code", 404)
                                .put("message",
                                        (routingContext.failure() != null) ?
                                                routingContext.failure().getMessage() :
                                                "Not Found"
                                );
                        routingContext
                                .response()
                                .setStatusCode(404)
                                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                                .end(errorObject.encode()); // <4>
                    });
                    router.errorHandler(400, routingContext -> {
                        JsonObject errorObject = new JsonObject()
                                .put("code", 400)
                                .put("message",
                                        (routingContext.failure() != null) ?
                                                routingContext.failure().getMessage() :
                                                "Validation Exception"
                                );
                        routingContext
                                .response()
                                .setStatusCode(400)
                                .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                                .end(errorObject.encode());
                    });

                    server = vertx.createHttpServer(new HttpServerOptions().setPort(8080).setHost("localhost"));
                    server.requestHandler(router).listen();
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    private static JsonObject source2Json(SourceDataset dataset) {
        DataSourceConfiguration sourceConfig = dataset.getSource().getConfiguration();
        JsonObject base = JsonObject.mapFrom(sourceConfig);
        base.put("sourceName",dataset.getName().getDisplay());
        if (sourceConfig instanceof FileSourceConfiguration) {
            base.put("objectType", "FileSourceConfig");
        } else throw new UnsupportedOperationException("Unexpected source config: " + sourceConfig.getClass());
        return base;
    }

    private enum Operation { ADD, UPDATE }

    @AllArgsConstructor
    private class SourceOperationHandler<S extends DataSourceConfiguration> implements Handler<RoutingContext> {

        private final Class<S> clazz;
        private final Operation operation;

        @Override
        public void handle(RoutingContext routingContext) {
            RequestParameters params = routingContext.get(ValidationHandler.REQUEST_CONTEXT_KEY);
            JsonObject source = params.body().getJsonObject();
            S sourceConfig = source.mapTo(clazz);
            ProcessMessage.ProcessBundle errors = new ProcessMessage.ProcessBundle<>();
            sourceConfig.validate(errors);
            if (errors.isFatal()) {
                routingContext.fail(405, new Exception(errors.combineMessages(ProcessMessage.Severity.FATAL,
                        "Provided configuration has the following validation errors:\n","\n" )));
            } else {
                switch (operation) {
                    case ADD:
                        environment.getDatasetRegistry().addSource(sourceConfig);
                        break;
                    case UPDATE:
                        environment.getDatasetRegistry().updateSource(sourceConfig);
                        break;
                    default: throw new UnsupportedOperationException();
                }

                routingContext
                        .response()
                        .setStatusCode(200)
                        .end();
            }
        }
    }



    @Override
    public void stop(){
        this.server.close();
    }

}
