package ai.dataeng.sqml.config.server;

import ai.dataeng.sqml.Environment;
import ai.dataeng.sqml.ScriptDeployment;
import ai.dataeng.sqml.config.util.StringNamedId;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
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

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

@Slf4j
public class ApiVerticle extends AbstractVerticle {

    public static final int DEFAULT_PORT = 5070;

    private static final Map<Integer,String> ERROR2MESSAGE = ImmutableMap.of(
            400, "Processing Error",
            404, "Not Found"
    );

    static final int VALIDATION_ERR_CODE = 405;

    private HttpServer server;

    private final Environment environment;
    private final int port = DEFAULT_PORT;

    public ApiVerticle(Environment environment) {
        this.environment = environment;
    }

    @Override
    public void start(Promise<Void> startPromise) {
        SourceHandler sourceHandler = new SourceHandler(environment.getDatasetRegistry());
        DeploymentHandler deployHandler = new DeploymentHandler(environment);
        RouterBuilder.create(this.vertx, "datasqrl-openapi.yml")
                .onSuccess(routerBuilder -> {
                    // #### Source handlers
                    routerBuilder.operation("addOrUpdateSource").handler(handleException(sourceHandler.update()));
                    routerBuilder.operation("getSources").handler(sourceHandler.get());
                    routerBuilder.operation("getSourceByName").handler(sourceHandler.getSourceByName());
                    routerBuilder.operation("deleteSource").handler(sourceHandler.deleteSource());
//                    routerBuilder.operation("updateSourceTable").handler();
//                    routerBuilder.operation("getSourceTables").handler();
//                    routerBuilder.operation("getSourceTableByName").handler();
//                    routerBuilder.operation("deleteSourceTable").handler();
//                    routerBuilder.operation("getSourceTables").handler();

                    // #### Script Submission handlers
                    routerBuilder.operation("getDeployments").handler(deployHandler.getDeployments());
                    routerBuilder.operation("deployScript").handler(handleException(deployHandler.deploy()));
                    routerBuilder.operation("getDeploymentById").handler(deployHandler.getDeploymentById());

                    Router router = routerBuilder.createRouter(); // <1>
                    //Generate generic error handlers
                    for (Map.Entry<Integer,String> failure : ERROR2MESSAGE.entrySet()) {
                        int errorCode = failure.getKey();
                        Preconditions.checkArgument(errorCode>=400 && errorCode<410);
                        String defaultMessage = failure.getValue();
                        Preconditions.checkArgument(StringUtils.isNotEmpty(defaultMessage));

                        router.errorHandler(errorCode, routingContext -> {
                            Throwable exception = routingContext.failure();
                            JsonObject errorObject = new JsonObject()
                                    .put("code", errorCode)
                                    .put("message", exception!=null?exception.getMessage():defaultMessage
                                    );
                            routingContext
                                    .response()
                                    .setStatusCode(errorCode)
                                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                                    .end(errorObject.encode());
                        });
                    }
                    server = vertx.createHttpServer(new HttpServerOptions().setPort(port).setHost("localhost"));
                    server.requestHandler(router).listen();
                    startPromise.complete();
                })
                .onFailure(startPromise::fail);
    }

    static Handler<RoutingContext> handleException(Handler<RoutingContext> handler) {
        return routingContext -> {
            try {
                handler.handle(routingContext);
            } catch (Throwable ex) {
                routingContext.fail(400, ex);
            }
        };
    }

    @Override
    public void stop(){
        this.server.close();
    }

}
