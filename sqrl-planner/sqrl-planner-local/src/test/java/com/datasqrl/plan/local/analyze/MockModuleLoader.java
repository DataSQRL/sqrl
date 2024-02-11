package com.datasqrl.plan.local.analyze;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.impl.file.FileDataSystemFactory;
import com.datasqrl.io.impl.print.PrintDataSystemFactory;
import com.datasqrl.loaders.*;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.module.TableNamespaceObject;
import org.apache.flink.connector.file.table.FileSystemConnectorOptions;

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
      return Optional.of(new SqrlDirectoryModule(
          applyTestSettings(objLoader.load(namePath))));
    }

    if (tables.containsKey(namePath)) {
      return Optional.of(tables.get(namePath));
    }

    //todo: move this out
    if (isPrintSink(namePath)) {
      DataSystemDiscovery sinks = PrintDataSystemFactory.getDefaultDiscovery();
      return Optional.of(
          new SqrlDirectoryModule(
              List.of(new DataSystemNsObject(namePath,sinks))));
    }

    if (isOutputSink(namePath)) {
      DataSystemDiscovery output = FileDataSystemFactory.getFileSinkConfig(errorDir.get()).build().initializeDiscovery();
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

  @Override
  public void add(NamePath apiNamePath, SqrlModule module) {

  }

  private List<NamespaceObject> applyTestSettings(List<NamespaceObject> objects) {
    for (NamespaceObject object : objects) {
      if (object instanceof TableSourceNamespaceObject) {
        TableSourceNamespaceObject s = (TableSourceNamespaceObject) object;
        s.getTable().getConfiguration()
            .getConnectorConfig()
            .setProperty("monitorIntervalMs" /*file-io not guaranteed to be on classpath*/, "0");
      }
    }

    return objects;
  }


  private static boolean isPrintSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
        .equals(PrintDataSystemFactory.SYSTEM_NAME);
  }
  private static boolean isOutputSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
        .equals("output");
  }

}
