package ai.datasqrl.config.server;

import ai.datasqrl.environment.Environment;
import ai.datasqrl.config.error.ErrorCollector;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.openapi.RouterBuilder;
import io.vertx.json.schema.ValidationException;
import io.vertx.json.schema.common.ValidationExceptionImpl;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ApiVerticle extends AbstractVerticle {

  public static final int DEFAULT_PORT = 5070;

  private static final Map<Integer, String> ERROR2MESSAGE = ImmutableMap.of(
      400, "Processing Error",
      404, "Not Found",
      500, "Server Error"
  );

  static final int VALIDATION_ERR_CODE = 400;
  private final SourceHandler sourceHandler;

  private HttpServer server;

  private final Environment environment;
  private final int port = DEFAULT_PORT;

//  public ApiVerticle(Environment environment) {
//    this.environment = environment;
//    sourceHandler = new SourceHandler(environment.getDatasetRegistry());
//  }

  public ApiVerticle(Environment environment, SourceHandler sourceHandler) {
    this.environment = environment;
    this.sourceHandler = sourceHandler;
  }

  @Override
  public void start(Promise<Void> startPromise) {
    SinkHandler sinkHandler = new SinkHandler(environment.getDataSinkRegistry());
    DeploymentHandler deployHandler = new DeploymentHandler(environment);
    RouterBuilder.create(this.vertx, "datasqrl-openapi.yml")
        .onSuccess(routerBuilder -> {
          // #### Source handlers
          routerBuilder.operation("addOrUpdateSource").handler(sourceHandler.update());
          routerBuilder.operation("getSources").handler(sourceHandler.get());
          routerBuilder.operation("getSourceByName").handler(sourceHandler.getSourceByName());
          routerBuilder.operation("deleteSource").handler(sourceHandler.deleteSource());
          routerBuilder.operation("addSourceTable").handler(sourceHandler.addTable());
          routerBuilder.operation("getSourceTables").handler(sourceHandler.getTables());
          routerBuilder.operation("getSourceTableByName").handler(sourceHandler.getTableByName());
          routerBuilder.operation("deleteSourceTable").handler(sourceHandler.deleteTable());

          // ### Sink Handlers
          routerBuilder.operation("addOrUpdateSink").handler(handleException(sinkHandler.update()));
          routerBuilder.operation("getSinks").handler(sinkHandler.get());
          routerBuilder.operation("getSinkByName").handler(sinkHandler.getSinkByName());
          routerBuilder.operation("deleteSink").handler(sinkHandler.deleteSink());

          // #### Script Submission handlers
          routerBuilder.operation("getDeployments").handler(deployHandler.getDeployments());
          routerBuilder.operation("deployScript").handler(handleException(deployHandler.deploy()));
          routerBuilder.operation("getDeploymentById").handler(deployHandler.getDeploymentById());
          routerBuilder.operation("compileScript").handler(deployHandler.compile());
          //Get and delete individual deployment

          Router router = routerBuilder.createRouter(); // <1>
          //Generate generic error handlers
          for (Map.Entry<Integer, String> failure : ERROR2MESSAGE.entrySet()) {
            int errorCode = failure.getKey();
            Preconditions.checkArgument(errorCode >= 400 && errorCode <= 500);
            String defaultMessage = failure.getValue();

            router.errorHandler(errorCode, routingContext -> {
              Throwable ex = routingContext.failure();
              if (ex.getCause() != null && ex.getCause() instanceof ValidationException) {
                //Validation error
                ErrorCollector errors = ErrorCollector.root();
                errors.fatal((ValidationException) ex.getCause());
                HandlerUtil.returnError(routingContext, errors);
              } else {
                //Generic errors
                String msg = ex != null ? ex.getMessage() : defaultMessage;
                //Re-map 400 errors to 500 since all 400 errors should be validation errors (i.e. handled above)
                int adjustedErrorCode = errorCode == 400 ? 500 : errorCode;
                JsonObject errorObject = new JsonObject()
                    .put("code", adjustedErrorCode)
                    .put("message", msg);
                routingContext
                    .response()
                    .setStatusCode(adjustedErrorCode)
                    .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
                    .end(errorObject.encode());
              }
            });
          }
          server = vertx.createHttpServer(
              new HttpServerOptions().setPort(port).setHost("0.0.0.0"));
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
        if (ex.getCause() != null && ex.getCause() instanceof ValidationExceptionImpl) {
          ErrorCollector errors = ErrorCollector.root();
          errors.fatal((ValidationExceptionImpl) ex.getCause());
          HandlerUtil.returnError(routingContext, errors);
        } else {
          routingContext.fail(400, ex);
        }
      }
    };
  }

  @Override
  public void stop() {
    this.server.close();
  }

}
