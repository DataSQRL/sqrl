package ai.datasqrl.config.server;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.error.ErrorMessage;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HandlerUtil {

  static JsonArray errors2Json(ErrorCollector errors) {
    List<ErrorMessage> msgs = errors.getAll();
    return getJsonArray(msgs, JsonObject::mapFrom);
  }


  static void returnError(RoutingContext routingContext, ErrorCollector errors) {
    JsonArray errorObject = errors2Json(errors);
    routingContext
        .response()
        .setStatusCode(ApiVerticle.VALIDATION_ERR_CODE)
        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .end(errorObject.encode());
  }

  static void returnResult(RoutingContext routingContext, String result) {
    routingContext
        .response()
        .setStatusCode(200)
        .putHeader(HttpHeaders.CONTENT_TYPE, "application/json")
        .end(result);
  }

  static void returnResult(RoutingContext routingContext, JsonObject result) {
    returnResult(routingContext, result.encode());
  }

  static void returnResult(RoutingContext routingContext, JsonArray result) {
    returnResult(routingContext, result.encode());
  }

  static <T> JsonArray getJsonArray(Collection<T> elements, Function<T, JsonObject> converter) {
    return getJsonArray(elements.stream(), converter);
  }

  static <T> JsonArray getJsonArray(Stream<T> elements, Function<T, JsonObject> converter) {
    List<JsonObject> arr = elements
        .map(converter).collect(Collectors.toList());
    return new JsonArray(arr);
  }

}
