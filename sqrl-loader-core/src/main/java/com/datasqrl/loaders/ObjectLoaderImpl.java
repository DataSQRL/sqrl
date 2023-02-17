package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystem;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.FlinkUdfNsObject;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import org.apache.flink.table.functions.UserDefinedFunction;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ObjectLoaderImpl implements ObjectLoader {

  ResourceResolver resourceResolver;
  ErrorCollector errors;

  final static Deserializer deserializer = new Deserializer();

  @Override
  public List<NamespaceObject> load(NamePath basePath) {
    List<URI> allItems = resourceResolver.loadPath(basePath);
    return allItems.stream()
            .flatMap(url -> load(url, basePath).stream())
            .collect(Collectors.toList());
  }

  private List<? extends NamespaceObject> load(URI uri, NamePath basePath) {
    if (uri.getPath().endsWith(DataSource.TABLE_FILE_SUFFIX)) {
      return loadTable(uri, basePath);
    } else if (uri.getPath().endsWith(".function.json")) {
      return loadFunction(uri, basePath);
    } else if (uri.getPath().endsWith(DataSource.DATASYSTEM_FILE)) {
      return loadDataSystem(uri, basePath);
    }
    return List.of();
  }

  private Optional<NamespaceObject> loadManfiest(URI uri, NamePath basePath) {
    return null;
  }

  private List<NamespaceObject> loadDataSystem(URI uri, NamePath basePath) {
    DataSystemConfig dataSystemConfig = deserializer.mapJsonFile(Path.of(uri), DataSystemConfig.class);

    DataSystem dataSystem = dataSystemConfig.initialize(errors);
    return List.of(new DataSystemNsObject(basePath, dataSystem));
  }

  /**
   * Used only for testing
   * @param namePath
   * @return
   */
  public TableSourceNamespaceObject loadTable(NamePath namePath) {
    NamePath basePath = namePath.popLast();
    return resourceResolver.loadPath(basePath).stream()
            .filter(uri -> uri.getPath().endsWith(namePath.getLast().getCanonical() + DataSource.TABLE_FILE_SUFFIX))
            .flatMap(uri -> loadTable(uri, basePath).stream()).findFirst().get();
  }

  private List<TableSourceNamespaceObject> loadTable(URI uri, NamePath basePath) {
    TableConfig tableConfig = deserializer.mapJsonFile(Path.of(uri), TableConfig.class);

    String schemaType = tableConfig.getSchema();
    errors.checkFatal(!Strings.isNullOrEmpty(schemaType), "Schema has not been configured for table [%s]", uri);
    Optional<TableSchemaFactory> tsfOpt = ServiceLoaderDiscovery.findFirst(TableSchemaFactory.class, tsf -> tsf.getType(), schemaType);
    errors.checkFatal(tsfOpt.isPresent(), "Could not find schema factory [%s] for table [%s]", schemaType, uri);
    TableSchemaFactory tableSchemaFactory = tsfOpt.get();

    Optional<TableSchema> tableSchema = tableSchemaFactory.create(uri, tableConfig, deserializer, errors);

    return new DataSource().readTableSource(tableSchema.get(), tableConfig, errors, basePath)
            .map(TableSourceNamespaceObject::new).map(List::of).orElse(List.of());
  }
  private static final Class<?> UDF_FUNCTION_CLASS = UserDefinedFunction.class;

  @SneakyThrows
  private List<NamespaceObject> loadFunction(URI uri, NamePath namePath) {
    ObjectNode json = deserializer.mapJsonFile(Path.of(uri), ObjectNode.class);
    String jarPath = json.get("jarPath").asText();
    String functionClassName = json.get("functionClass").asText();

    URL jarUrl = new File(jarPath).toURI().toURL();
    Class<?> functionClass = loadClass(jarPath, functionClassName);
    Preconditions.checkArgument(UDF_FUNCTION_CLASS.isAssignableFrom(functionClass), "Class is not a UserDefinedFunction");

    UserDefinedFunction udf = (UserDefinedFunction) functionClass.getDeclaredConstructor().newInstance();

    // Return a namespace object containing the created function
    return List.of(new FlinkUdfNsObject(Name.system(udf.getClass().getSimpleName()), udf, Optional.of(jarUrl)));
  }

  @SneakyThrows
  private Class<?> loadClass(String jarPath, String functionClassName) {
    URL[] urls = {new File(jarPath).toURI().toURL()};
    URLClassLoader classLoader = new URLClassLoader(urls, Thread.currentThread().getContextClassLoader());
    return Class.forName(functionClassName, true, classLoader);
  }

}
