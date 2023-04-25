package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import java.util.Optional;
import javax.validation.constraints.NotEmpty;
import lombok.NonNull;

public class LoaderUtil {


  public static TableSink loadSink(@NonNull @NotEmpty NamePath sinkPath, ErrorCollector errors,
        ModuleLoader moduleLoader) {
      Optional<TableSink> sink = moduleLoader.getModule(sinkPath.popLast())
        .flatMap(m -> m.getNamespaceObject(sinkPath.popLast().getLast()))
        .map(s -> ((DataSystemNsObject) s).getDiscovery())
        .flatMap(dataSystem -> dataSystem.discoverSink(sinkPath.getLast(), errors))
        .map(tblConfig ->
            tblConfig.initializeSink(errors, sinkPath, Optional.empty()));

    errors.checkFatal(sink.isPresent(), ErrorCode.CANNOT_RESOLVE_TABLESINK,
        "Cannot resolve table sink: %s", sinkPath);
    return sink.get();
  }



}
