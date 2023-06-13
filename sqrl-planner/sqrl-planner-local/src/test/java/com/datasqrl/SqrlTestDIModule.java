package com.datasqrl;

import static com.datasqrl.loaders.LoaderUtil.loadSink;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.frontend.ErrorSink;
import com.datasqrl.frontend.SqrlDIModule;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.plan.local.analyze.MockModuleLoader;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public class SqrlTestDIModule extends SqrlDIModule {

  public SqrlTestDIModule(ExecutionPipeline pipeline,
      IntegrationTestSettings settings, Path rootDir, Map<NamePath, SqrlModule> addlModules,
      Optional<Path> errorDir, ErrorCollector errors) {
    super(pipeline,
        settings.getDebugger(),
        createModuleLoader(rootDir, addlModules, errors, errorDir),
        createErrorSink(settings.getErrorSink(), errors,
            createModuleLoader(rootDir, addlModules, errors, errorDir)),
        errors);
  }

  private static ErrorSink createErrorSink(NamePath errorSink, ErrorCollector errors,
      ModuleLoader moduleLoader) {
    return new ErrorSink(loadSink(errorSink, errors, moduleLoader));
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


}