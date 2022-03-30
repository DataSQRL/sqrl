package ai.dataeng.sqml.config.server;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.error.ErrorMessage;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.DataSourceUpdate;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import java.util.*;
import java.util.stream.Collectors;

@AllArgsConstructor
public class SourceHandler {

    private final DatasetRegistry registry;

    Handler<RoutingContext> update() {
        return routingContext -> {
            RequestParameters params = routingContext.get(ValidationHandler.REQUEST_CONTEXT_KEY);
            JsonObject source = params.body().getJsonObject();

            ErrorCollector errors = ErrorCollector.root();
            SourceDataset result = null;

            DataSourceUpdate update = source.mapTo(DataSourceUpdate.class);
            result = registry.addOrUpdateSource(update, errors);

            if (result == null) {
                HandlerUtil.returnError(routingContext, errors);
            } else {
                HandlerUtil.returnResult(routingContext, source2Json(result, errors));
            }
        };
    }

    Handler<RoutingContext> get() {
        return routingContext -> {
            HandlerUtil.returnResult(routingContext,HandlerUtil.getJsonArray(registry.getDatasets(), SourceHandler::source2Json));
        };
    }

    Handler<RoutingContext> getSourceByName() {
        return routingContext -> {
            RequestParameters params = routingContext.get("parsedParameters");
            String sourceName = params.pathParameter("sourceName").getString();
            SourceDataset ds = registry.getDataset(sourceName);
            if (ds!=null) {
                HandlerUtil.returnResult(routingContext, source2Json(ds));
            } else {
                routingContext.fail(404, new Exception("Source not found"));
            }
        };
    }


    Handler<RoutingContext> deleteSource() {
        return routingContext -> {
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
        };
    }

    static JsonObject source2Json(@NonNull SourceDataset source) {
        return source2Json(source,null);
    }


    static JsonObject source2Json(@NonNull SourceDataset source, ErrorCollector errors) {
        return JsonObject.mapFrom(new DataSourceResult(source, errors));
    }




    @Value
    @AllArgsConstructor
    static class DataSourceResult {

        private String name;

        private DataSourceConfiguration config;

        List<SourceTableConfiguration> tables;

        List<ErrorMessage> messages;

        DataSourceResult(@NonNull SourceDataset dataset) {
            this(dataset,null);
        }

        DataSourceResult(@NonNull SourceDataset dataset, ErrorCollector errors) {
            this(dataset.getName().getDisplay(),
                    dataset.getSource().getConfiguration(),
                    dataset.getTables().stream().map(SourceTable::getConfiguration).collect(Collectors.toList()),
                    errors==null?null:errors.getAll());

        }

    }

}
