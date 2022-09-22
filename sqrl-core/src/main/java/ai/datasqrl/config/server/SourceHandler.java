package ai.datasqrl.config.server;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.config.error.ErrorMessage;
import ai.datasqrl.io.sources.DataSourceConfiguration;
import ai.datasqrl.io.sources.DataSourceImplementation;
import ai.datasqrl.io.sources.DataSourceUpdate;
import ai.datasqrl.io.sources.SourceTableConfiguration;
import ai.datasqrl.io.sources.dataset.DatasetRegistry;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.parse.tree.name.Name;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.validation.RequestParameters;
import io.vertx.ext.web.validation.ValidationHandler;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.Value;
import org.apache.commons.lang3.tuple.Pair;

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

  public Handler<RoutingContext> get() {
    return routingContext -> {
      HandlerUtil.returnResult(routingContext,
          HandlerUtil.getJsonArray(registry.getDatasets(), SourceHandler::source2Json));
    };
  }

  public Handler<RoutingContext> getSourceByName() {
    return routingContext -> {
      SourceDataset ds = getDataset(routingContext);
      if (ds != null) {
        HandlerUtil.returnResult(routingContext, source2Json(ds));
      }
    };
  }

  private SourceDataset getDataset(RoutingContext routingContext) {
    RequestParameters params = routingContext.get("parsedParameters");
    String sourceName = params.pathParameter("sourceName").getString();
    SourceDataset ds = registry.getDataset(Name.system(sourceName));
    if (ds == null) {
      routingContext.fail(404, new Exception("Source not found"));
      return null;
    }
    return ds;
  }

  Handler<RoutingContext> deleteSource() {
    return routingContext -> {
      RequestParameters params = routingContext.get("parsedParameters");
      String sourceName = params.pathParameter("sourceName").getString();
      Pair<SourceDataset, Collection<SourceTable>> removal = registry.removeSource(sourceName);
      if (removal != null) {
        HandlerUtil.returnResult(routingContext,
            source2Json(removal.getKey(), removal.getValue(), null));
      } else {
        routingContext.fail(404, new Exception("Source not found"));
      }
    };
  }

  static JsonObject source2Json(@NonNull SourceDataset source) {
    return source2Json(source, null);
  }

  static JsonObject source2Json(@NonNull SourceDataset source, ErrorCollector errors) {
    return source2Json(source, source.getTables(), errors);
  }

  static JsonObject source2Json(@NonNull SourceDataset source,
      @NonNull Collection<SourceTable> tables, ErrorCollector errors) {
    return JsonObject.mapFrom(new DataSourceResult(source, tables, errors));
  }

  Handler<RoutingContext> addTable() {
    return routingContext -> {
      SourceDataset dataset = getDataset(routingContext);
      if (dataset != null) {
        RequestParameters params = routingContext.get(ValidationHandler.REQUEST_CONTEXT_KEY);
        JsonObject tbl = params.body().getJsonObject();

        ErrorCollector errors = ErrorCollector.root();

        SourceTableConfiguration tblConfig = tbl.mapTo(SourceTableConfiguration.class);
        SourceTable table = dataset.addTable(tblConfig, errors);
        if (table == null) {
          HandlerUtil.returnError(routingContext, errors);
        } else {
          HandlerUtil.returnResult(routingContext, table2Json(table, errors));
        }
      }
    };
  }

  Handler<RoutingContext> getTables() {
    return routingContext -> {
      SourceDataset dataset = getDataset(routingContext);
      if (dataset != null) {
        HandlerUtil.returnResult(routingContext,
            HandlerUtil.getJsonArray(dataset.getTables(), SourceHandler::table2Json));
      }
    };
  }

  Handler<RoutingContext> getTableByName() {
    return routingContext -> {
      SourceDataset dataset = getDataset(routingContext);
      if (dataset != null) {
        RequestParameters params = routingContext.get("parsedParameters");
        String tableName = params.pathParameter("tableName").getString();
        SourceTable table = dataset.getTable(tableName);
        if (table != null) {
          HandlerUtil.returnResult(routingContext, table2Json(table));
        } else {
          routingContext.fail(404, new Exception("Table not found"));
        }
      }
    };
  }

  Handler<RoutingContext> deleteTable() {
    return routingContext -> {
      SourceDataset dataset = getDataset(routingContext);
      if (dataset != null) {
        RequestParameters params = routingContext.get("parsedParameters");
        String tableName = params.pathParameter("tableName").getString();
        SourceTable table = dataset.removeTable(tableName);
        if (table != null) {
          HandlerUtil.returnResult(routingContext, table2Json(table));
        } else {
          routingContext.fail(404, new Exception("Table not found"));
        }
      }
    };
  }


  static JsonObject table2Json(@NonNull SourceTable table) {
    return table2Json(table, null);
  }

  static JsonObject table2Json(@NonNull SourceTable source, ErrorCollector errors) {
    JsonObject table = JsonObject.mapFrom(source.getConfiguration());
    if (errors != null) {
      JsonArray msgs = HandlerUtil.getJsonArray(errors.getAll(), JsonObject::mapFrom);
      table.put("messages", msgs);
    }
    return table;
  }

  @Value
  @AllArgsConstructor
  static class DataSourceResult {

    String name;
    DataSourceImplementation source;
    DataSourceConfiguration config;
    List<SourceTableConfiguration> tables;

    List<ErrorMessage> messages;

    DataSourceResult(@NonNull SourceDataset dataset, Collection<SourceTable> tables,
        ErrorCollector errors) {
      this(dataset.getName().getDisplay(),
          dataset.getSource().getImplementation(),
          dataset.getSource().getConfig(),
          tables.stream().map(SourceTable::getConfiguration).collect(Collectors.toList()),
          errors == null ? null : errors.getAll());

    }

  }

}
