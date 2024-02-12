package com.datasqrl.plan.local.analyze;

import com.datasqrl.TestModuleFactory;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.module.resolver.FileResourceResolver;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.datasqrl.plan.table.TableConverter;
import com.datasqrl.plan.table.TableIdFactory;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;
import lombok.experimental.Delegate;

@Singleton
public class MockModuleLoaderImpl implements ModuleLoader {
  @Delegate
  ModuleLoader loader;

  @Inject
  public MockModuleLoaderImpl(
      @Nullable @Named("rootDir") Path rootDir,
      @Named("errorDir") Optional<Path> errorDir,
      CalciteTableFactory tableFactory,
      ErrorCollector errors,
      @Nullable @Named("addlModules") Map<NamePath, SqrlModule> tables) {
    if (rootDir != null) {
      ObjectLoaderImpl objectLoader = new ObjectLoaderImpl(new FileResourceResolver(rootDir),
          errors, tableFactory);
      loader = new MockModuleLoader(objectLoader, Map.of(), errorDir);
    } else if (tables != null) {
      loader = new MockModuleLoader(null, tables, errorDir);
    } else {
      Map<NamePath, SqrlModule> addlModules = TestModuleFactory
          .merge(TestModuleFactory.createRetail(tableFactory), TestModuleFactory.createFuzz(tableFactory));
      loader = new MockModuleLoader(null, addlModules, errorDir);
    }
  }
}
