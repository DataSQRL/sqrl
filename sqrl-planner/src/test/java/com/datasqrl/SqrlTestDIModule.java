package com.datasqrl;

import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCode;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.frontend.SqrlDIModule;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.DataSystemNsObject;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.analyze.MockModuleLoader;
import com.datasqrl.plan.local.generate.FileResourceResolver;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import javax.validation.constraints.NotEmpty;
import lombok.NonNull;

public class SqrlTestDIModule extends SqrlDIModule {

  public SqrlTestDIModule(ExecutionPipeline pipeline,
      IntegrationTestSettings settings, Path rootDir, Map<NamePath, SqrlModule> addlModules,
      Optional<Path> errorDir, ErrorCollector errors) {
    super(pipeline,
        settings.getDebugger(),
        createModuleLoader(rootDir, addlModules, errors, errorDir),
        createErrorSink(settings.getErrorSink(), errors,
            createModuleLoader(rootDir, addlModules, errors, errorDir))
        );
  }

  private static ErrorSink createErrorSink(NamePath errorSink, ErrorCollector errors,
      ModuleLoader moduleLoader) {
    return new ErrorSink(loadErrorSink(errorSink, errors, moduleLoader));
  }

  private static ModuleLoader createModuleLoader(Path rootDir, Map<NamePath, SqrlModule> addlModules,
      ErrorCollector errors, Optional<Path> errorDir) {
    if (rootDir != null) {
      ObjectLoaderImpl objectLoader = new ObjectLoaderImpl(new FileResourceResolver(rootDir),
          errors);
      return new MockModuleLoader(objectLoader, addlModules, errorDir);
    } else {
      return new MockModuleLoader(null, addlModules, errorDir);
    }
  }

  private static TableSink loadErrorSink(@NonNull @NotEmpty NamePath sinkPath, ErrorCollector error,
      ModuleLoader ml) {
    Optional<TableSink> errorSink = ml
        .getModule(sinkPath.popLast())
        .flatMap(m->m.getNamespaceObject(sinkPath.popLast().getLast()))
        .map(s -> ((DataSystemNsObject) s).getTable())
        .flatMap(dataSystem -> dataSystem.discoverSink(sinkPath.getLast(), error))
        .map(tblConfig ->
            tblConfig.initializeSink(error, sinkPath, Optional.empty()));
    error.checkFatal(errorSink.isPresent(), ErrorCode.CANNOT_RESOLVE_TABLESINK,
        "Cannot resolve table sink: %s", errorSink);

    return errorSink.get();
  }
}