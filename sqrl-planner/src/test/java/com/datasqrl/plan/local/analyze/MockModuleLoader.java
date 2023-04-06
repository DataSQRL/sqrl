package com.datasqrl.plan.local.analyze;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystem;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.DataSystemDiscoveryConfig;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.formats.FileFormat;
import com.datasqrl.io.impl.file.DirectoryDataSystemConfig;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.loaders.DataSystemNsObject;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.loaders.SqrlDirectoryModule;
import com.datasqrl.loaders.SqrlModule;
import com.datasqrl.loaders.StandardLibraryLoader;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.NamespaceObject;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MockModuleLoader implements ModuleLoader {

  private final StandardLibraryLoader standardLibraryLoader;
  private final Map<NamePath, SqrlModule> tables;
  private final ObjectLoaderImpl objLoader;
  private final Optional<Path> errorDir;

  public MockModuleLoader(ObjectLoaderImpl objLoader, Map<NamePath, SqrlModule> tables,
      Optional<Path> errorDir) {
    this.objLoader = objLoader;
    this.errorDir = errorDir;
    this.standardLibraryLoader = new StandardLibraryLoader();
    this.tables = tables;
  }

  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    if (objLoader != null && !objLoader.load(namePath).isEmpty()) {
      return Optional.of(new SqrlDirectoryModule(objLoader.load(namePath)));
    }

    if (tables.containsKey(namePath)) {
      return Optional.of(tables.get(namePath));
    }

    //todo: move this out
    if (isPrintSink(namePath)) {
      DataSystemConfig config = PrintDataSystem.DEFAULT_DISCOVERY_CONFIG;
      ErrorCollector errors = ErrorCollector.root();
      DataSystem dataSystem = config.initialize(errors);
      return Optional.of(
          new SqrlDirectoryModule(
              List.of(new DataSystemNsObject(namePath,dataSystem))));
    }

    if (isOutputSink(namePath)) {
      DataSystem output = createOutputModule();
      return Optional.of(
          new SqrlDirectoryModule(
              List.of(new DataSystemNsObject(namePath,output))));
    }

    List<NamespaceObject> objects = standardLibraryLoader.load(namePath);
    if (objects.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        new SqrlDirectoryModule(objects));
  }

  public DataSystem createOutputModule() {
    DataSystemDiscoveryConfig datasystem = DirectoryDataSystemConfig.ofDirectory(errorDir.get());
    DataSystemConfig.DataSystemConfigBuilder builder = DataSystemConfig.builder();
    builder.datadiscovery(datasystem);
    builder.type(ExternalDataType.sink);
    builder.name("output");
    builder.format(FileFormat.JSON.getImplementation().getDefaultConfiguration());
    DataSystemConfig config = builder.build();

    return config.initialize(ErrorCollector.root());
  }

  private static boolean isPrintSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
        .equals(PrintDataSystem.SYSTEM_TYPE);
  }
  private static boolean isOutputSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
        .equals("output");
  }

  @Override
  public String toString() {
    return objLoader.toString();
  }
}
