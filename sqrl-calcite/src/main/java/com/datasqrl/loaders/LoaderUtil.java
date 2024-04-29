package com.datasqrl.loaders;

import static com.datasqrl.config.ConnectorFactoryFactory.PRINT_SINK_NAME;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSinkImpl;
import java.util.Map;
import java.util.Optional;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class LoaderUtil {

  public static Optional<TableSink> loadSinkOpt(@NonNull NamePath sinkPath, ErrorCollector errors,
        ModuleLoader moduleLoader, ConnectorFactoryFactory connectorFactoryFactory) {

      if (sinkPath.getFirst().getCanonical().equalsIgnoreCase(PRINT_SINK_NAME)) {
        ConnectorFactory print = connectorFactoryFactory.create(null, PRINT_SINK_NAME)
            .get();
        TableConfig sourceAndSink = print.createSourceAndSink(new ConnectorFactoryContext(
            Map.of("name", sinkPath.getLast().getDisplay())));
        return Optional.of(TableSinkImpl.create(sourceAndSink, sinkPath, Optional.empty()));
      }

    //Attempt to load and convert a table sink
    Optional<TableSink> sink2 = moduleLoader.getModule(sinkPath.popLast())
        .flatMap(m -> m.getNamespaceObject(sinkPath.getLast()))
        .filter(t->t instanceof TableSinkObject)
        .map(t->((TableSinkObject) t).getSink());

    return sink2;
  }

  public TableSink initializeSink(TableConfig tableConfig, NamePath basePath, Optional<TableSchema> schema) {
//    getErrors().checkFatal(getBase().getType().isSink(), "Table is not a sink: %s", name);
    Name tableName = tableConfig.getName();
    return new TableSinkImpl(tableConfig, basePath.concat(tableName), tableName, schema);
  }
}
