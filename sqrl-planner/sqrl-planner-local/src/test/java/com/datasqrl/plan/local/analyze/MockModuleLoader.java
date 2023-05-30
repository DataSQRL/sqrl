package com.datasqrl.plan.local.analyze;

import static com.datasqrl.loaders.ObjectLoaderImpl.FUNCTION_JSON;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.impl.print.PrintDataSystemFactory;
import com.datasqrl.loaders.DataSource;
import com.datasqrl.loaders.DataSystemNsObject;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ModuleMetadata;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.loaders.ObjectLoaderMetadata;
import com.datasqrl.loaders.SqrlDirectoryModule;
import com.datasqrl.loaders.StandardLibraryLoader;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.SneakyThrows;

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

  private static boolean isPrintSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
        .equals(PrintDataSystemFactory.SYSTEM_NAME);
  }

  private static boolean isOutputSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
        .equals("output");
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
      DataSystemDiscovery sinks = PrintDataSystemFactory.getDefaultDiscovery();
      return Optional.of(
          new SqrlDirectoryModule(
              List.of(new DataSystemNsObject(namePath, sinks))));
    }

    if (isOutputSink(namePath)) {
      DataSystemDiscovery output = FileDataSystemFactory.getFileSinkConfig(errorDir.get()).build()
          .initializeDiscovery();
      return Optional.of(
          new SqrlDirectoryModule(
              List.of(new DataSystemNsObject(namePath, output))));
    }

    List<NamespaceObject> objects = standardLibraryLoader.load(namePath);
    if (objects.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        new SqrlDirectoryModule(objects));
  }

  @SneakyThrows
  @Override
  public ModuleMetadata getModuleMetadata(NamePath path) {
    return new ModuleMetadata(path,
        objLoader == null ? new ObjectLoaderMetadata(List.of(
            new URI("/example/uri/found.datasystem.json")
        ), Path.of("example").resolve("uri"),
            List.of(DataSource.DATASYSTEM_FILE, DataSource.TABLE_FILE_SUFFIX, FUNCTION_JSON)) :
            objLoader.getMetadata(path.popLast()));
  }
}
