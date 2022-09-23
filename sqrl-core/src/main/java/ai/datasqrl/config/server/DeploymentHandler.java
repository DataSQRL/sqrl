package ai.datasqrl.config.server;

import ai.datasqrl.config.error.ErrorLocation;
import ai.datasqrl.config.error.ErrorLocation.File;
import ai.datasqrl.config.error.ErrorLocationImpl;
import ai.datasqrl.config.error.ErrorMessage.Severity;
import ai.datasqrl.environment.Environment;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.error.ErrorMessage;
import ai.datasqrl.config.scripts.ScriptBundle.Config;
import ai.datasqrl.config.util.StringNamedId;
import ai.datasqrl.environment.ScriptDeployment;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public
class DeploymentHandler {

  private final Environment environment;

  public Handler<RoutingContext> deploy() {
    return routingContext -> {
      RequestParameters params = routingContext.get(ValidationHandler.REQUEST_CONTEXT_KEY);
      JsonObject bundleJson = params.body().getJsonObject();
      Config bundleConfig = bundleJson.mapTo(Config.class);
      ErrorCollector errors = ErrorCollector.root();
      // for go testing - doesn't work
//      ErrorLocation testLoc = new ErrorLocationImpl("file://", new File(10, 7), "example.sqrl");
//      ErrorMessage testMsg = new ErrorMessage.Implementation("Column `id` ambiguous", testLoc, Severity.FATAL);
//      errors.add(testMsg);
      // end go testing
      ScriptDeployment.Result result = environment.deployScript(bundleConfig, errors);
      if (errors.isFatal() || result == null) {
        // original code, commented for testing
//        routingContext.fail(405, new Exception(errors.combineMessages(ErrorMessage.Severity.FATAL,
//           "Provided bundle has the following validation errors:\n", "\n")));
        // for go testing - run with sqrl deploy [script] in main.go
        String JSONErrorExtended = "[{\"message\":\"Column `id` ambiguous\","
            + "\"severity\":\"fatal\","
            + "\"location\":{"
            + "\"prefix\":\"file://\","
            + "\"path\":\"example/example.sqrl\","
            + "\"file\":{"
            + "\"line\":10,"
            + "\"offset\":7,"
            + "\"context\": {"
            + "\"text\": \"SELECT id, lastName, price, quantity, sku FROM customers c JOIN orders o ON c.orderNo = o.orderNo\","
            + "\"highlight_start\": 7,"
            + "\"highlight_end\": 9"
            + "}}}}]";
        routingContext
            .response()
            .setStatusCode(405)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .end(JSONErrorExtended);
        // end go testing
      } else {
        JsonObject jsonResult = deploymentResult2Json(result);
        routingContext
            .response()
            .setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .end(jsonResult.encode());
      }
    };
  }

  public Handler<RoutingContext> compile() {
    return null; //TODO: implement
  }

  public Handler<RoutingContext> getDeployments() {
    return routingContext -> {
      List<JsonObject> sources = environment.getActiveDeployments().stream()
          .map(DeploymentHandler::deploymentResult2Json).collect(Collectors.toList());
      routingContext
          .response()
          .setStatusCode(200)
          .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
          .end(new JsonArray(sources).encode());
    };
  }

  public Handler<RoutingContext> getDeploymentById() {
    return routingContext -> {
      RequestParameters params = routingContext.get("parsedParameters");
      String submitId = params.pathParameter("deployId").getString();
      Optional<ScriptDeployment.Result> result = environment.getDeployment(
          StringNamedId.of(submitId));
      if (result.isPresent()) {
        routingContext
            .response()
            .setStatusCode(200)
            .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
            .end(deploymentResult2Json(result.get()).encode());
      } else {
        routingContext.fail(404, new Exception("Deployment not found"));
      }
    };
  }

  private static JsonObject deploymentResult2Json(ScriptDeployment.Result result) {
    JsonObject base = JsonObject.mapFrom(result);
    return base;
  }
}
