package com.datasqrl.loaders;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.engine.stream.flink.PrintFlinkDynamicSinkConnectorFactory;
import com.datasqrl.io.StandardDynamicSinkFactory;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.google.inject.Inject;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class ModuleLoaderImpl implements ModuleLoader {

  public static final String PRINT_SINK_NAME = "print";
  final StandardLibraryLoader standardLibraryLoader = new StandardLibraryLoader();
  private final ResourceResolver resourceResolver;
  private final ErrorCollector errors;
  private final CalciteTableFactory tableFactory;
  private final SqrlFramework framework;

  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    // Load modules from standard library
    List<NamespaceObject> nsObjects = new ArrayList<>(loadFromStandardLibrary(namePath));

    // Attempt to load flink sql
    if (nsObjects.isEmpty()) {
      nsObjects = new FlinkSqlLoader(framework, resourceResolver, tableFactory, this).load(namePath);
    }

    // Load modules from file system
    if (nsObjects.isEmpty()) {
      nsObjects.addAll(loadFromFileSystem(namePath));
    }

    if (nsObjects.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(new SqrlDirectoryModule(nsObjects));
  }

  public static boolean isPrintSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
            .equals(PRINT_SINK_NAME);
  }


  private List<NamespaceObject> loadFromStandardLibrary(NamePath namePath) {
    if (isPrintSink(namePath)) {
      return List.of(new DynamicSinkNsObject(namePath, new StandardDynamicSinkFactory(new PrintFlinkDynamicSinkConnectorFactory(), SqrlConfig.createCurrentVersion())));
    }

    return standardLibraryLoader.load(namePath);
  }

  private List<NamespaceObject> loadFromFileSystem(NamePath namePath) {
    return new ObjectLoaderImpl(resourceResolver, errors, tableFactory, this).load(namePath);
  }

  @Override
  public String toString() {
    return new ObjectLoaderImpl(resourceResolver, errors, tableFactory, this).toString();
  }

}
