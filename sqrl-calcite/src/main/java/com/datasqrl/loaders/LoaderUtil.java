package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import java.util.Optional;
import lombok.NonNull;
import lombok.experimental.UtilityClass;

@UtilityClass
public class LoaderUtil {

  public static Optional<TableSink> loadSinkOpt(@NonNull NamePath sinkPath, ErrorCollector errors,
        ModuleLoader moduleLoader) {
      Optional<TableSink> sink = moduleLoader.getModule(sinkPath.popLast())
        .flatMap(m -> m.getNamespaceObject(sinkPath.popLast().getLast()))
        .map(s -> ((DynamicSinkNsObject) s).getSinkFactory())
        .map(dynamicSink -> dynamicSink.get(sinkPath.getLast()))
        .map(tblConfig ->
            tblConfig.initializeSink(sinkPath, Optional.empty()));

      //Attempt to load and convert a table sink
      if (sink.isEmpty()) {
        Optional<TableSink> sink2 = moduleLoader.getModule(sinkPath.popLast())
            .flatMap(m -> m.getNamespaceObject(sinkPath.getLast()))
            .filter(t->t instanceof TableSinkObject)
            .map(t->((TableSinkObject) t).getSink());
        if (sink2.isPresent()) {
          return sink2;
        }
      }

    return sink;
  }
}
