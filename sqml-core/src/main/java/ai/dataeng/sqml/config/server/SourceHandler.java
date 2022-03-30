package ai.dataeng.sqml.config.server;

import ai.dataeng.sqml.config.error.ErrorCollector;
import ai.dataeng.sqml.config.error.ErrorMessage;
import ai.dataeng.sqml.io.sources.DataSource;
import ai.dataeng.sqml.io.sources.DataSourceConfiguration;
import ai.dataeng.sqml.io.sources.DataSourceUpdate;
import ai.dataeng.sqml.io.sources.SourceTableConfiguration;
import ai.dataeng.sqml.io.sources.dataset.DatasetRegistry;
import ai.dataeng.sqml.io.sources.dataset.SourceDataset;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.formats.CSVFormat;
import ai.dataeng.sqml.io.sources.formats.FormatConfiguration;
import ai.dataeng.sqml.io.sources.formats.JsonLineFormat;
import ai.dataeng.sqml.io.sources.impl.file.FileSourceConfiguration;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import lombok.AllArgsConstructor;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
                HandlerUtil.returnError(routingContext,errors);
            } else {
                HandlerUtil.returnResult(routingContext, source2Json(result, errors));
            }
        };
    }

    Handler<RoutingContext> get() {
        return routingContext -> {
            HandlerUtil.returnResult(routingContext,HandlerUtil.getJsonArray(registry.getDatasets(),SourceHandler::source2Json));
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

    static JsonObject source2Json(@NonNull SourceDataset dataset) {
        return source2Json(dataset,null);
    }

    static JsonObject source2Json(@NonNull SourceDataset dataset, ErrorCollector errors) {
        return source2Json(dataset.getName().getDisplay(),dataset.getSource().getConfiguration(),
                dataset.getTables().stream().map(SourceTable::getConfiguration).collect(Collectors.toList()), errors);
    }

    static JsonObject source2Json(String name, @NonNull DataSourceConfiguration config, @NonNull List<SourceTableConfiguration> tables,
                                  ErrorCollector errors) {
        JsonObject res = new JsonObject();
        if (!Strings.isNullOrEmpty(name)) res.put("name",name);
        String sourceType;
        if (config instanceof FileSourceConfiguration) sourceType = "file";
        else throw new UnsupportedOperationException("Unexpected source config: " + config.getClass());
        res.put("sourceType",sourceType);
        res.put("config",JsonObject.mapFrom(config));
        if (!tables.isEmpty()) res.put("tables",HandlerUtil.getJsonArray(tables, JsonObject::mapFrom));
        if (errors!=null) res.put("messages",HandlerUtil.errors2Json(errors));
        return res;
    }

    public static JsonObject source2Json(String name, @NonNull DataSourceConfiguration config, @NonNull List<SourceTableConfiguration> tables) {
        return source2Json(name,config,tables,null);
    }


    static SourceTableConfiguration json2Table(JsonObject tbl, ErrorCollector errors) {
        SourceTableConfiguration.SourceTableConfigurationBuilder tblBuilder = SourceTableConfiguration.builder();
        String name = tbl.getString("name");
        String format = tbl.getString("format").trim().toLowerCase();
        errors = errors.resolve(name);
        tblBuilder.name(name);
        tblBuilder.format(format);
        tblBuilder.identifier(tbl.getString("identifier",null));

        if (tbl.containsKey("formatConfig")) {
            Class<? extends FormatConfiguration> formatClass = FORMAT_CONFIG_MAPPING.get(format);
            if (formatClass == null) {
                errors.fatal("Not a valid format: %s",format);
                return null;
            } else {
                FormatConfiguration config = tbl.getJsonObject("formatConfig").mapTo(formatClass);
                tblBuilder.formatConfig(config);
            }
        }
        return tblBuilder.build();
    }
}
