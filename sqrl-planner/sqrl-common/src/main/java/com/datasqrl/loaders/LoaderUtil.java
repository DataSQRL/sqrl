package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.io.tables.TableSource;
import java.util.Optional;
import lombok.NonNull;

public class LoaderUtil {


  public static TableSink loadSink(@NonNull NamePath sinkPath, ErrorCollector errors,
        ModuleLoader moduleLoader) {
      Optional<TableSink> sink = moduleLoader.getModule(sinkPath.popLast())
        .flatMap(m -> m.getNamespaceObject(sinkPath.popLast().getLast()))
        .map(s -> ((DataSystemNsObject) s).getDiscovery())
        .flatMap(dataSystem -> dataSystem.discoverSink(sinkPath.getLast(), errors))
        .map(tblConfig ->
            tblConfig.initializeSink(sinkPath, Optional.empty()));

      //Attempt to load and convert a table sink
      if (sink.isEmpty()) {
        Optional<TableSink> sink2 = moduleLoader.getModule(sinkPath.popLast())
            .flatMap(m -> m.getNamespaceObject(sinkPath.getLast()))
            .filter(t->t instanceof TableSinkObject)
            .map(t->((TableSinkObject) t).getSink());
        if (sink2.isPresent()) {
          return sink2.get();
        }
      }

    errors.checkFatal(sink.isPresent(), ErrorCode.CANNOT_RESOLVE_TABLESINK,
        "Cannot resolve table sink: %s", sinkPath);
    return sink.get();
  }



}
