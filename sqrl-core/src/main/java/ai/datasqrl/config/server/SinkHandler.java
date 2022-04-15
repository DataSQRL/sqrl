package ai.datasqrl.config.server;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.io.sinks.DataSink;
import ai.datasqrl.io.sinks.DataSinkRegistration;
import ai.datasqrl.io.sinks.registry.DataSinkRegistry;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import lombok.AllArgsConstructor;
import lombok.NonNull;

@AllArgsConstructor
public class SinkHandler {

    private final DataSinkRegistry registry;

    Handler<RoutingContext> update() {
        return routingContext -> {
            RequestParameters params = routingContext.get(ValidationHandler.REQUEST_CONTEXT_KEY);
            JsonObject source = params.body().getJsonObject();

            ErrorCollector errors = ErrorCollector.root();

            DataSinkRegistration sinkReg = source.mapTo(DataSinkRegistration.class);
            DataSink sink = registry.addOrUpdateSink(sinkReg, errors);

            if (sink == null) {
                HandlerUtil.returnError(routingContext, errors);
            } else {
                HandlerUtil.returnResult(routingContext, sink2Json(sink, errors));
            }
        };
    }

    Handler<RoutingContext> get() {
        return routingContext -> {
            HandlerUtil.returnResult(routingContext,HandlerUtil.getJsonArray(registry.getSinks(), SinkHandler::sink2Json));
        };
    }

    Handler<RoutingContext> getSinkByName() {
        return routingContext -> {
            RequestParameters params = routingContext.get("parsedParameters");
            String sinkName = params.pathParameter("sinkName").getString();
            DataSink sink = registry.getSink(sinkName);
            if (sink != null) {
                HandlerUtil.returnResult(routingContext, sink2Json(sink));
            } else {
                routingContext.fail(404, new Exception("Sink not found"));
            }
        };
    }

    Handler<RoutingContext> deleteSink() {
        return routingContext -> {
            RequestParameters params = routingContext.get("parsedParameters");
            String sinkName = params.pathParameter("sinkName").getString();
            DataSink sink = registry.removeSink(sinkName);
            if (sink != null) {
                HandlerUtil.returnResult(routingContext, sink2Json(sink));
            } else {
                routingContext.fail(404, new Exception("Sink not found"));
            }
        };
    }

    static JsonObject sink2Json(@NonNull DataSink sink) {
        return sink2Json(sink,null);
    }

    static JsonObject sink2Json(@NonNull DataSink sink, ErrorCollector errors) {
        JsonObject table = JsonObject.mapFrom(sink.getRegistration());
        if (errors != null) {
            JsonArray msgs = HandlerUtil.getJsonArray(errors.getAll(), JsonObject::mapFrom);
            table.put("messages",msgs);
        }
        return table;
    }


}
