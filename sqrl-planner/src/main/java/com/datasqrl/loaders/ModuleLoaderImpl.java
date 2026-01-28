/*
 * Copyright © 2021 DataSQRL (contact@datasqrl.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datasqrl.loaders;

import static com.google.common.base.Preconditions.checkArgument;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.BuildPath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.FlinkUdfNsObject;
import com.datasqrl.loaders.FlinkTableNamespaceObject.FlinkTable;
import com.datasqrl.loaders.resolver.ResourceResolver;
import com.datasqrl.loaders.schema.SchemaLoader;
import com.datasqrl.loaders.schema.SchemaLoaderImpl;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.FunctionUtil;
import com.datasqrl.util.StringUtil;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import jakarta.inject.Inject;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.table.functions.UserDefinedFunction;
import org.springframework.stereotype.Component;

@Component
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class ModuleLoaderImpl implements ModuleLoader {

  public static final String TABLE_FILE_SUFFIX = ".table.sql";
  public static final String FUNCTION_JSON_EXTENSION = ".function.json";
  public static final String SQRL_FILE_EXTENSION = ".sqrl";
  static final Deserializer SERIALIZER = Deserializer.INSTANCE;

  private final ClasspathFunctionLoader classpathFunctionLoader;
  private final BuildPath buildPath;
  private final ResourceResolver resourceResolver;
  private final ErrorCollector errors;
  private final Cache<NamePath, SqrlModule> cache =
      CacheBuilder.newBuilder().maximumSize(1000).build();
  ;

  @Inject
  public ModuleLoaderImpl(
      ResourceResolver resourceResolver, BuildPath buildPath, ErrorCollector errors) {
    this.classpathFunctionLoader = new ClasspathFunctionLoader();
    this.buildPath = buildPath;
    this.resourceResolver = resourceResolver;
    this.errors = errors;
  }

  private ModuleLoaderImpl withResourceResolver(ResourceResolver resourceResolver) {
    return new ModuleLoaderImpl(classpathFunctionLoader, buildPath, resourceResolver, errors);
  }

  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    var cached = cache.getIfPresent(namePath);
    if (cached != null) {
      return Optional.of(cached);
    }

    var module = getModuleOpt(namePath);
    module.ifPresent(sqrlModule -> cache.put(namePath, sqrlModule));

    return module;
  }

  private Optional<SqrlModule> getModuleOpt(NamePath namePath) {
    // Load modules from file system first
    var module = loadFromFileSystem(namePath);
    if (module.isEmpty()) { // if it's not local, try to load it from classpath
      module = loadFunctionsFromClasspath(namePath);
    }
    return module;
  }

  private Optional<SqrlModule> loadFunctionsFromClasspath(NamePath namePath) {
    var lib = classpathFunctionLoader.load(namePath);
    if (lib.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new SqrlDirectoryModule(lib));
  }

  private Optional<SqrlModule> loadFromFileSystem(NamePath directory) {
    // Folders take precedence
    var allItems = resourceResolver.loadPath(directory);

    // Check for sqrl scripts
    if (allItems.isEmpty()) {
      var sqrlFile =
          getFile(
              directory.popLast(),
              Name.system(directory.getLast().toString() + SQRL_FILE_EXTENSION));
      if (sqrlFile.isPresent()) {
        return Optional.of(loadScript(directory, sqrlFile.get()));
      }
    }

    List<NamespaceObject> items =
        allItems.stream()
            .flatMap(url -> load(url, directory).stream())
            .collect(Collectors.toList());

    return items.isEmpty() ? Optional.empty() : Optional.of(new SqrlDirectoryModule(items));
  }

  private Optional<Path> getFile(NamePath directory, Name name) {
    return resourceResolver.resolveFile(directory.concat(name));
  }

  private List<? extends NamespaceObject> load(Path path, NamePath directory) {
    var filename = path.getFileName().toString();

    if (filename.endsWith(TABLE_FILE_SUFFIX)) {
      return loadTable(path);
    }

    if (filename.endsWith(SQRL_FILE_EXTENSION)) {
      var script =
          directory.concat(Name.system(StringUtil.removeFromEnd(filename, SQRL_FILE_EXTENSION)));
      return List.of(loadScript(script, path).asNamespaceObject());
    }

    if (filename.endsWith(FUNCTION_JSON_EXTENSION)) {
      return loadFunction(path, directory);
    }

    return List.of();
  }

  @SneakyThrows
  private ScriptSqrlModule loadScript(NamePath namePath, Path path) {
    return new ScriptSqrlModule(
        Files.readString(path),
        path,
        namePath,
        withResourceResolver(resourceResolver.getNested(path.getParent())));
  }

  @SneakyThrows
  private List<FlinkTableNamespaceObject> loadTable(Path path) {
    var tableName = StringUtil.removeFromEnd(path.getFileName().toString(), TABLE_FILE_SUFFIX);
    errors.checkFatal(Name.validName(tableName), "Not a valid table name: %s", tableName);

    var tableSql = Files.readString(path);

    return List.of(
        new FlinkTableNamespaceObject(
            new FlinkTable(Name.system(tableName), tableSql, path),
            new SchemaLoaderImpl(resourceResolver.getNested(path.getParent()), errors)));
  }

  @Override
  public SchemaLoader getSchemaLoader() {
    return new SchemaLoaderImpl(resourceResolver, errors);
  }

  public static final Class<?> UDF_FUNCTION_CLASS = UserDefinedFunction.class;

  @SneakyThrows
  private List<NamespaceObject> loadFunction(Path path, NamePath namePath) {
    var json = SERIALIZER.mapJsonFile(path, ObjectNode.class);
    var jarPath = json.get("jarPath").asText();
    var functionClassName = json.get("functionClass").asText();
    var resolvedJarPath = buildPath.getUdfPath().resolve(jarPath);
    var jarUrl = resolvedJarPath.toUri().toURL();
    var functionClass = loadClass(jarUrl, functionClassName);
    checkArgument(
        UDF_FUNCTION_CLASS.isAssignableFrom(functionClass), "Class is not a UserDefinedFunction");

    UserDefinedFunction udf =
        (UserDefinedFunction) functionClass.getDeclaredConstructor().newInstance();

    // Return a namespace object containing the created function
    String functionName = FunctionUtil.getFunctionName(udf);

    return List.of(new FlinkUdfNsObject(functionName, udf, functionName, Optional.of(jarUrl)));
  }

  @SneakyThrows
  private Class<?> loadClass(URL jarPath, String functionClassName) {
    URL[] urls = {jarPath};
    var classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());

    return Class.forName(functionClassName, true, classLoader);
  }

  @Override
  public String toString() {
    return resourceResolver.toString();
  }
}
