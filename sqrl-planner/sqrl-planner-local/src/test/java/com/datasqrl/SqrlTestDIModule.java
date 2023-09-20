package com.datasqrl;

import static com.datasqrl.loaders.LoaderUtil.loadSink;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NameCanonicalizer;
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
import com.datasqrl.plan.table.CalciteTableFactory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

public class SqrlTestDIModule extends SqrlDIModule {

  public SqrlTestDIModule(ExecutionPipeline pipeline,
      IntegrationTestSettings settings, Path rootDir, Map<NamePath, SqrlModule> addlModules,
      Optional<Path> errorDir, ErrorCollector errors, SqrlFramework framework,
      NameCanonicalizer nameCanonicalizer) {
    super(pipeline,
        settings.getDebugger(),
        createModuleLoader(rootDir, addlModules, errors, errorDir, new CalciteTableFactory(framework,
            nameCanonicalizer)),
        createErrorSink(settings.getErrorSink(), errors,
            createModuleLoader(rootDir, addlModules, errors, errorDir, new CalciteTableFactory(framework,
                nameCanonicalizer))),
        errors, framework, NameCanonicalizer.SYSTEM);
  }

  private static ErrorSink createErrorSink(NamePath errorSink, ErrorCollector errors,
      ModuleLoader moduleLoader) {
    return new ErrorSink(loadSink(errorSink, errors, moduleLoader));
  }

  private static ModuleLoader createModuleLoader(Path rootDir, Map<NamePath, SqrlModule> addlModules,
      ErrorCollector errors, Optional<Path> errorDir, CalciteTableFactory tableFactory) {
    if (rootDir != null) {
      ObjectLoaderImpl objectLoader = new ObjectLoaderImpl(new FileResourceResolver(rootDir),
          errors, tableFactory);
      return new MockModuleLoader(objectLoader, addlModules, errorDir);
    } else {
      return new MockModuleLoader(null, addlModules, errorDir);
    }
  }


}