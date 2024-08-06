package com.datasqrl.plan.local.analyze;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.loaders.ModuleLoader;
import com.datasqrl.loaders.ObjectLoaderImpl;
import com.datasqrl.loaders.SqrlDirectoryModule;
import com.datasqrl.loaders.StandardLibraryLoader;
import com.datasqrl.loaders.TableSourceNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
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
//
//    //todo: move this out
//    if (ModuleLoaderImpl.isPrintSink(namePath) ) {
//      if (true) throw new RuntimeException();
//      return Optional.of(
//          new SqrlDirectoryModule(
//              List.of(new DynamicSinkNsObject(namePath, new StandardDynamicSinkFactory(new PrintFlinkDynamicSinkConnectorFactory()
//                  ,
////                  SqrlConfig.createCurrentVersion()
//                  null
//              )))));
//    }
//
//    if (isOutputSink(namePath)) {
//      return Optional.of(
//          new SqrlDirectoryModule(
//              List.of(new DynamicSinkNsObject(namePath, FileFlinkDynamicSinkConnectorFactory.forPath(errorDir.get())))));
//    }

    List<NamespaceObject> objects = standardLibraryLoader.load(namePath);
    if (objects.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(
        new SqrlDirectoryModule(objects));
  }

  private List<NamespaceObject> applyTestSettings(List<NamespaceObject> objects) {
    for (NamespaceObject object : objects) {
      if (object instanceof TableSourceNamespaceObject) {
        TableSourceNamespaceObject s = (TableSourceNamespaceObject) object;

      }
    }

    return objects;
  }

  private static boolean isOutputSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
        .equals("output");
  }

}
